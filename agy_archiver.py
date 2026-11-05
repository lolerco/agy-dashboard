#!/usr/bin/env python3
"""AGY Usage Tracker & Archiver Daemon (Zero-Dependency Python 3.9+).

Solves the ephemeral transcript loss problem for Hermes Agent & Antigravity (AGY):
1. Scans and immediately HARDLINKS active transcripts from Hermes ephemeral
   scratch directories (~/.hermes/cache/scratch/hermes_agy_*) and standalone
   Antigravity directories (~/.gemini/antigravity-cli/brain).
   Hardlinks share physical disk inodes, so the session transcript survives
   Hermes deleting its temporary directory on exit and continues receiving live writes.
2. Uses zero-CPU Linux inotify kernel events (via ctypes/libc) for maximum power
   and battery savings while idling.
3. Distinguishes Hermes vs. Standalone AGY sessions and deduplicates conversations
   via UUID primary keys in SQLite so sessions are never double-counted.
4. Detects session termination (scratch dir removed or inactivity threshold reached).
5. Triggers agy_dashboard.py indexing so all tokens, costs, models, and tool calls
   are safely committed to SQLite (~/.agy_dashboard/<hostname>.db).
6. Zips completed transcripts into ~/.agy_dashboard/transcripts_archive.zip
   (storage optimization) and unlinks the uncompressed raw files to keep disk space clean.
"""

from __future__ import annotations

import argparse
import ctypes
import os
import select
import shutil
import sqlite3
import struct
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Path Resolution & Defaults
# ---------------------------------------------------------------------------

def resolve_dashboard_dir(custom_path: Optional[str] = None) -> Path:
    if custom_path:
        p = Path(custom_path).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p

    env_dir = os.environ.get("AGY_DASHBOARD_DIR", "").strip()
    if env_dir:
        p = Path(env_dir).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p

    # Standard default is ~/.agy_dashboard
    home_dash = Path.home() / ".agy_dashboard"
    if home_dash.is_dir():
        return home_dash

    # Container or alternative workspace environment fallback
    ws_dash = Path("/workspace/.agy_dashboard")
    if ws_dash.is_dir():
        return ws_dash

    home_dash.mkdir(parents=True, exist_ok=True)
    return home_dash


TRANSCRIPT_NAMES = ("transcript.jsonl", "transcript_full.jsonl")


def get_scan_roots() -> List[Path]:
    """Return all roots where Hermes or AGY transcripts may be spawned."""
    candidates: List[Path] = [
        Path.home() / ".hermes" / "cache" / "scratch",
        Path.home() / ".gemini" / "antigravity-cli" / "brain",
        Path.home() / ".hermes" / "profiles",
        Path("/workspace/.hermes/cache/scratch"),
        Path("/workspace/.gemini/antigravity-cli/brain"),
        Path("/workspace/.hermes/profiles"),
    ]
    unique: List[Path] = []
    seen: Set[str] = set()
    for c in candidates:
        try:
            res = str(c.resolve())
        except OSError:
            res = str(c)
        if res not in seen and c.is_dir():
            seen.add(res)
            unique.append(c)
    return unique


# ---------------------------------------------------------------------------
# Human Formatting Helpers
# ---------------------------------------------------------------------------

def fmt_bytes(num_bytes: int | float) -> str:
    if num_bytes <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    val = float(num_bytes)
    idx = 0
    while val >= 1024.0 and idx < len(units) - 1:
        val /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(val)} B"
    return f"{val:.1f} {units[idx]}"


# ---------------------------------------------------------------------------
# Inotify Kernel Event Listener (Power-Saving Linux stdlib via ctypes)
# ---------------------------------------------------------------------------

class LinuxInotifyWatcher:
    """Zero-battery-drain kernel event listener via Linux inotify & select."""

    IN_CREATE = 0x00000100
    IN_DELETE = 0x00000200
    IN_MODIFY = 0x00000002
    IN_MOVED_FROM = 0x00000040
    IN_MOVED_TO = 0x00000080
    IN_NONBLOCK = 0x00000800
    IN_CLOEXEC = 0x00080000

    def __init__(self) -> None:
        self.fd: Optional[int] = None
        self.watch_descriptors: Dict[int, str] = {}
        try:
            self.libc = ctypes.CDLL("libc.so.6", use_errno=True)
            init_fn = getattr(self.libc, "inotify_init1", None)
            if init_fn:
                self.fd = init_fn(self.IN_NONBLOCK | self.IN_CLOEXEC)
            else:
                self.fd = self.libc.inotify_init()
        except Exception:
            self.fd = None

    @property
    def is_available(self) -> bool:
        return self.fd is not None and self.fd >= 0

    def add_watch(self, path: Path) -> int:
        if not self.is_available or not path.is_dir():
            return -1
        mask = (
            self.IN_CREATE
            | self.IN_MODIFY
            | self.IN_DELETE
            | self.IN_MOVED_TO
            | self.IN_MOVED_FROM
        )
        try:
            c_path = str(path).encode("utf-8")
            wd = self.libc.inotify_add_watch(self.fd, c_path, mask)
            if wd >= 0:
                self.watch_descriptors[wd] = str(path)
            return wd
        except Exception:
            return -1

    def add_watch_tree(self, root: Path, max_depth: int = 8) -> int:
        """Recursively attach watches to root and all existing subdirectories."""
        if not self.is_available or not root.is_dir():
            return 0
        added = 0
        if self.add_watch(root) >= 0:
            added += 1
        try:
            for dirpath, dirnames, _ in os.walk(str(root)):
                dirnames[:] = [d for d in dirnames if d not in (".git", "node_modules", "__pycache__", "archive")]
                rel_depth = len(Path(dirpath).parts) - len(root.parts)
                if rel_depth > max_depth:
                    dirnames[:] = []
                    continue
                p = Path(dirpath)
                if p != root and self.add_watch(p) >= 0:
                    added += 1
        except OSError:
            pass
        return added

    def wait_for_events(self, timeout_sec: float) -> bool:
        """Sleep in kernel select until inotify events arrive. Return True if events occurred."""
        if not self.is_available or self.fd is None:
            time.sleep(timeout_sec)
            return True
        try:
            r, _, _ = select.select([self.fd], [], [], timeout_sec)
            if not r:
                return False
            try:
                buf = os.read(self.fd, 16384)
            except OSError:
                return True

            # Parse event structs to dynamically watch newly created directories
            offset = 0
            event_fmt = "iIII"
            event_size = struct.calcsize(event_fmt)
            while offset + event_size <= len(buf):
                wd, mask, _cookie, name_len = struct.unpack_from(event_fmt, buf, offset)
                offset += event_size
                name = ""
                if name_len > 0:
                    raw_name = buf[offset : offset + name_len]
                    offset += name_len
                    name = raw_name.split(b"\x00", 1)[0].decode("utf-8", errors="ignore")

                # Dynamically attach inotify watches to newly created subdirectories
                if (mask & self.IN_ISDIR) and (mask & (self.IN_CREATE | self.IN_MOVED_TO)):
                    parent_path_str = self.watch_descriptors.get(wd)
                    if parent_path_str:
                        new_dir = Path(parent_path_str) / name
                        if new_dir.is_dir():
                            self.add_watch_tree(new_dir, max_depth=4)

            return True
        except Exception:
            time.sleep(timeout_sec)
            return True

    def close(self) -> None:
        if self.fd is not None and self.fd >= 0:
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = None
            self.watch_descriptors.clear()


# ---------------------------------------------------------------------------
# Archiver Core Implementation
# ---------------------------------------------------------------------------

class AgyArchiver:
    def __init__(
        self,
        dashboard_dir: Optional[Path] = None,
        zip_name: str = "transcripts_archive.zip",
        inactivity_threshold: float = 60.0,
        index_interval: float = 120.0,
        enable_zip: bool = True,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        self.dashboard_dir = dashboard_dir or resolve_dashboard_dir()
        self.staging_brain = self.dashboard_dir / "archive" / "brain"
        self.zip_path = self.dashboard_dir / zip_name
        self.dash_script = Path(__file__).resolve().parent / "agy_dashboard.py"
        self.inactivity_threshold = inactivity_threshold
        self.index_interval = index_interval
        self.enable_zip = enable_zip
        self.dry_run = dry_run
        self.verbose = verbose

        # State tracking
        self.active_convs: Dict[str, Dict[str, Any]] = {}
        self.last_index_time = 0.0

        self.staging_brain.mkdir(parents=True, exist_ok=True)

    def log(self, msg: str) -> None:
        if self.verbose:
            print(f"[AGY Archiver] {msg}")

    def find_logs_dirs(self, root: Path, depth: int = 0, max_depth: int = 8) -> Iterable[Path]:
        if depth > max_depth or not root.is_dir():
            return
        try:
            entries = list(os.scandir(root))
        except OSError:
            return
        for e in entries:
            if not e.is_dir(follow_symlinks=False):
                continue
            if e.name == "logs" and root.name == ".system_generated":
                yield Path(e.path)
            elif e.name in (".git", "node_modules", "__pycache__", "archive"):
                continue
            else:
                yield from self.find_logs_dirs(Path(e.path), depth + 1, max_depth)

    def link_file(self, src: Path, dst: Path) -> bool:
        """Hardlink or safely mirror src to dst without losing data."""
        try:
            if dst.exists():
                if os.path.samefile(src, dst):
                    return False
                # Different inode: preserve whichever is larger
                if dst.stat().st_size >= src.stat().st_size:
                    return False
                if not self.dry_run:
                    dst.unlink()

            if not self.dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                try:
                    os.link(src, dst)
                except OSError:
                    # Cross-device link fallback
                    shutil.copy2(src, dst)
            return True
        except OSError as exc:
            self.log(f"Error linking {src} -> {dst}: {exc}")
            return False

    def sweep(self) -> int:
        """Scan all roots and hardlink active transcript files into staging."""
        new_links = 0
        roots = get_scan_roots()
        for root in roots:
            for logs_dir in self.find_logs_dirs(root):
                # conv_dir is two levels above: <conv_uuid>/.system_generated/logs
                conv_dir = logs_dir.parent.parent
                conv_id = conv_dir.name

                # Ignore files already inside staging
                if conv_dir.parent == self.staging_brain:
                    continue

                # Discern source origin
                is_hermes = "hermes" in str(logs_dir).lower() or "scratch" in str(logs_dir).lower()
                source_tag = "hermes" if is_hermes else "agy_standalone"

                staged_conv_dir = self.staging_brain / conv_id
                staged_logs_dir = staged_conv_dir / ".system_generated" / "logs"

                max_mtime = 0.0
                total_size = 0
                has_files = False

                for name in TRANSCRIPT_NAMES:
                    src_file = logs_dir / name
                    if src_file.is_file():
                        has_files = True
                        st = src_file.stat()
                        max_mtime = max(max_mtime, st.st_mtime)
                        total_size += st.st_size
                        dst_file = staged_logs_dir / name
                        if self.link_file(src_file, dst_file):
                            new_links += 1

                if has_files:
                    self.active_convs[conv_id] = {
                        "source_tag": source_tag,
                        "source_dir": conv_dir,
                        "staged_dir": staged_conv_dir,
                        "last_seen_mtime": max_mtime,
                        "last_size": total_size,
                        "source_alive": conv_dir.is_dir(),
                    }
        return new_links

    def trigger_indexing(self) -> bool:
        """Invoke agy_dashboard.py to parse and persist transcripts into SQLite."""
        if not self.dash_script.is_file():
            self.log(f"Dashboard script not found at {self.dash_script}")
            return False

        cmd = [
            sys.executable,
            str(self.dash_script),
            "--agy-dir",
            str(self.staging_brain),
            "--summary",
            "--days",
            "all",
        ]
        try:
            res = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300,
                check=False,
            )
            self.last_index_time = time.time()
            return res.returncode == 0
        except Exception as exc:
            self.log(f"Failed to run indexer: {exc}")
            return False

    def get_indexed_conversation_ids(self) -> Set[str]:
        """Query all SQLite DBs in dashboard_dir to verify indexed conversation IDs."""
        indexed: Set[str] = set()
        db_candidates = list(self.dashboard_dir.glob("*.db"))
        for db_file in db_candidates:
            try:
                conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
                cursor = conn.execute("SELECT id FROM conversations")
                for (cid,) in cursor.fetchall():
                    indexed.add(cid)
                conn.close()
            except Exception:
                pass
        return indexed

    def zip_and_cleanup_completed(self, force_all: bool = False) -> Tuple[int, int]:
        """Compress verified indexed transcripts to transcripts_archive.zip and clean uncompressed files."""
        if not self.enable_zip:
            return 0, 0

        indexed_ids = self.get_indexed_conversation_ids()
        now = time.time()
        staged_conv_dirs = [d for d in self.staging_brain.iterdir() if d.is_dir()]

        to_archive: List[Tuple[str, Path]] = []
        for conv_dir in staged_conv_dirs:
            conv_id = conv_dir.name
            if conv_id not in indexed_ids:
                # Not yet confirmed in SQLite database; never delete premature transcripts!
                continue

            # Check session inactivity or termination
            meta = self.active_convs.get(conv_id)
            is_ready = force_all
            if not is_ready and meta:
                source_dir = meta.get("source_dir")
                source_alive = source_dir and Path(source_dir).is_dir()
                is_scratch = "hermes" in str(source_dir).lower() or "scratch" in str(source_dir).lower()
                if is_scratch:
                    # Ephemeral Hermes scratchpad: NEVER archive/unlink while Hermes session is still alive!
                    if not source_alive:
                        is_ready = True
                else:
                    # Standalone/permanent directory: safe after inactivity threshold
                    if now - meta.get("last_seen_mtime", 0) >= self.inactivity_threshold:
                        is_ready = True
            elif not is_ready:
                # If not tracked in active memory, check disk mtime
                logs = conv_dir / ".system_generated" / "logs"
                mtimes = [f.stat().st_mtime for f in logs.glob("*.jsonl") if f.is_file()]
                if mtimes and (now - max(mtimes) >= self.inactivity_threshold):
                    is_ready = True

            if is_ready:
                to_archive.append((conv_id, conv_dir))

        if not to_archive:
            return 0, 0

        if self.dry_run:
            self.log(f"[DRY RUN] Would zip and clean {len(to_archive)} conversations")
            return len(to_archive), 0

        # Check if any conversation already in the ZIP has updated (grown in size)
        needs_repack = False
        if self.zip_path.is_file():
            try:
                with zipfile.ZipFile(self.zip_path, "r") as zf_check:
                    zip_info_map = {zi.filename: zi for zi in zf_check.infolist()}
                    for conv_id, conv_dir in to_archive:
                        for jf in conv_dir.glob("**/*.jsonl"):
                            arcname = f"brain/{conv_id}/{jf.relative_to(conv_dir)}"
                            if arcname in zip_info_map:
                                if jf.stat().st_size != zip_info_map[arcname].file_size:
                                    needs_repack = True
                                    break
                        if needs_repack:
                            break
            except Exception:
                pass

        if needs_repack:
            return self._repack_zip_with_updated(to_archive)

        # Append to zip file
        zipped_files_count = 0
        cleaned_convs_count = 0

        try:
            with zipfile.ZipFile(self.zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zf:
                existing_zip_names = set(zf.namelist())
                for conv_id, conv_dir in to_archive:
                    jsonl_files = list(conv_dir.glob("**/*.jsonl"))
                    for jf in jsonl_files:
                        arcname = f"brain/{conv_id}/{jf.relative_to(conv_dir)}"
                        if arcname not in existing_zip_names:
                            zf.write(jf, arcname)
                            existing_zip_names.add(arcname)
                            zipped_files_count += 1

                    # Once written to zip, delete uncompressed files
                    for jf in jsonl_files:
                        try:
                            jf.unlink()
                        except OSError:
                            pass

                    # Remove empty directory tree
                    try:
                        shutil.rmtree(conv_dir)
                        cleaned_convs_count += 1
                    except OSError:
                        pass
        except Exception as exc:
            self.log(f"Error creating/updating zip {self.zip_path}: {exc}")

        return cleaned_convs_count, zipped_files_count

    def _repack_zip_with_updated(self, to_archive: List[Tuple[str, Path]]) -> Tuple[int, int]:
        """Atomically repack zip file when archived transcripts have grown in size."""
        temp_zip = self.zip_path.with_name(f"{self.zip_path.name}.tmp.{os.getpid()}")
        updated_conv_ids = {conv_id for conv_id, _ in to_archive}
        repacked_count = 0
        cleaned_convs = 0

        try:
            with zipfile.ZipFile(temp_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf_out:
                if self.zip_path.is_file():
                    with zipfile.ZipFile(self.zip_path, "r") as zf_in:
                        for item in zf_in.infolist():
                            parts = item.filename.split("/")
                            c_id = parts[1] if len(parts) > 1 else ""
                            if c_id not in updated_conv_ids:
                                zf_out.writestr(item, zf_in.read(item.filename))

                for conv_id, conv_dir in to_archive:
                    for jf in conv_dir.glob("**/*.jsonl"):
                        arcname = f"brain/{conv_id}/{jf.relative_to(conv_dir)}"
                        zf_out.write(jf, arcname)
                        repacked_count += 1

            temp_zip.replace(self.zip_path)

            for conv_id, conv_dir in to_archive:
                for jf in conv_dir.glob("**/*.jsonl"):
                    try:
                        jf.unlink()
                    except OSError:
                        pass
                try:
                    shutil.rmtree(conv_dir)
                    cleaned_convs += 1
                except OSError:
                    pass
            return cleaned_convs, repacked_count
        except Exception as exc:
            self.log(f"Error repacking zip: {exc}")
            if temp_zip.exists():
                try:
                    temp_zip.unlink()
                except OSError:
                    pass
            return 0, 0

    def get_status_report(self) -> Dict[str, Any]:
        indexed_ids = self.get_indexed_conversation_ids()
        staged_dirs = [d for d in self.staging_brain.iterdir() if d.is_dir()]
        staged_bytes = sum(
            f.stat().st_size
            for d in staged_dirs
            for f in d.glob("**/*")
            if f.is_file()
        )
        zip_bytes = self.zip_path.stat().st_size if self.zip_path.is_file() else 0
        zip_count = 0
        if self.zip_path.is_file():
            try:
                with zipfile.ZipFile(self.zip_path, "r") as zf:
                    zip_count = len(zf.namelist())
            except Exception:
                pass

        return {
            "dashboard_dir": str(self.dashboard_dir),
            "staging_dir": str(self.staging_brain),
            "staging_convs": len(staged_dirs),
            "staging_bytes": staged_bytes,
            "staging_human": fmt_bytes(staged_bytes),
            "indexed_db_convs": len(indexed_ids),
            "zip_path": str(self.zip_path),
            "zip_exists": self.zip_path.is_file(),
            "zip_bytes": zip_bytes,
            "zip_human": fmt_bytes(zip_bytes),
            "zip_files_count": zip_count,
        }

    def run_once(self) -> None:
        self.log("Starting single synchronization sweep...")
        linked = self.sweep()
        self.log(f"Sweep complete. Hardlinked {linked} active transcript files.")
        self.log("Triggering SQLite database indexing...")
        self.trigger_indexing()
        self.log("Zipping completed transcripts and freeing uncompressed disk space...")
        cleaned, zipped = self.zip_and_cleanup_completed(force_all=True)
        status = self.get_status_report()
        print(
            f"[AGY Archiver] Synced: {status['indexed_db_convs']} DB convs | "
            f"Zip archive: {status['zip_human']} ({status['zip_files_count']} files in {self.zip_path.name}) | "
            f"Staging remaining: {status['staging_convs']} convs ({status['staging_human']})"
        )

    def run_daemon(self, interval: float = 3.0) -> None:
        print(f"[AGY Archiver] Daemon started.")
        print(f"  Dashboard Dir : {self.dashboard_dir}")
        print(f"  Zip Target    : {self.zip_path}")
        print(f"  Power Saving  : Linux inotify kernel events (0% idle CPU)")
        
        watcher = LinuxInotifyWatcher()
        if watcher.is_available:
            for root in get_scan_roots():
                watcher.add_watch_tree(root)
            self.log(f"Attached inotify to {len(watcher.watch_descriptors)} scan directories")
        else:
            self.log("inotify not available; using adaptive low-power sleep")

        try:
            # Initial sweep & sync
            self.sweep()
            self.trigger_indexing()
            self.zip_and_cleanup_completed(force_all=False)

            while True:
                had_events = watcher.wait_for_events(interval)
                now = time.time()
                time_for_index = (now - self.last_index_time >= self.index_interval)

                if had_events:
                    self.sweep()

                if time_for_index:
                    if not had_events:
                        self.sweep()
                    self.trigger_indexing()
                    self.zip_and_cleanup_completed(force_all=False)
        except KeyboardInterrupt:
            print("\n[AGY Archiver] Shutting down gracefully...")
        finally:
            watcher.close()


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Zero-dependency background archiver & storage optimizer for AGY / Hermes transcripts.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single sweep, index, zip, and cleanup pass, then exit.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Display current storage footprint, SQLite index counts, and zip archive metrics.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=3.0,
        help="Inotify wait timeout or poll interval in seconds (default: 3.0)",
    )
    parser.add_argument(
        "--inactivity-threshold",
        type=float,
        default=60.0,
        help="Inactivity seconds before a session is zipped and cleaned (default: 60)",
    )
    parser.add_argument(
        "--index-interval",
        type=float,
        default=120.0,
        help="Interval in seconds between database indexing passes (default: 120)",
    )
    parser.add_argument(
        "--dashboard-dir",
        type=str,
        default=None,
        help="Custom .agy_dashboard directory path (default: ~/.agy_dashboard)",
    )
    parser.add_argument(
        "--zip-name",
        type=str,
        default="transcripts_archive.zip",
        help="Name of the zip archive file inside dashboard dir (default: transcripts_archive.zip)",
    )
    parser.add_argument(
        "--no-zip",
        action="store_true",
        help="Disable zipping; preserve uncompressed transcripts in staging.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate actions without linking, modifying database, or deleting files.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose diagnostic logging.",
    )

    args = parser.parse_args()

    archiver = AgyArchiver(
        dashboard_dir=Path(args.dashboard_dir) if args.dashboard_dir else None,
        zip_name=args.zip_name,
        inactivity_threshold=args.inactivity_threshold,
        index_interval=args.index_interval,
        enable_zip=not args.no_zip,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    if args.status:
        st = archiver.get_status_report()
        print("══════════════════════════════════════════════════════════════════════════════")
        print("  AGY TRANSCRIPT ARCHIVER & STORAGE METRICS")
        print("══════════════════════════════════════════════════════════════════════════════")
        print(f"  Dashboard Dir   : {st['dashboard_dir']}")
        print(f"  Indexed in DB   : {st['indexed_db_convs']} conversations")
        print(f"  Staging Unzipped: {st['staging_convs']} conversations ({st['staging_human']})")
        zip_desc = f"{st['zip_human']} ({st['zip_files_count']} files)" if st['zip_exists'] else "None (0 B)"
        print(f"  Zip Archive     : {st['zip_path']} [{zip_desc}]")
        print("──────────────────────────────────────────────────────────────────────────────")
        return

    if args.once:
        archiver.run_once()
    else:
        archiver.run_daemon(interval=args.interval)


if __name__ == "__main__":
    main()
