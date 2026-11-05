# AGY & Hermes Transcript Archiver

`agy_archiver.py` is a zero-dependency, ultra-low-power background daemon and CLI utility that ensures **no Antigravity (AGY) or Hermes Agent token logs are ever lost**, while optimizing disk storage by automatically compressing finished transcripts into a unified ZIP archive.

---

## Background: Why the Archiver Is Necessary

When you interact with Gemini models through **Hermes Agent** via the Antigravity direct SDK plugin, Hermes isolates each execution inside a temporary scratch directory:

```text
~/.hermes/cache/scratch/hermes_agy_<random_id>/home/.gemini/antigravity-cli/brain/<conversation_uuid>/
```

### The Problem
* **Graceful Hermes Exit (`/quit` or normal termination)**: Hermes automatically calls `temp_dir.cleanup()` and `rmtree()`, permanently unlinking the temporary scratchpad and deleting `transcript.jsonl` **before** any periodic dashboard scanner can read it.
* **Sudden Crash / SIGKILL**: The scratchpad directory is left on disk, but standard scanners historically missed them because they were located inside hidden directories (`.hermes/cache/scratch`).

`agy_archiver.py` solves this non-invasively—**without modifying any Hermes Agent source code**—by operating as an independent, kernel-event-driven watcher.

---

## Architecture & Lifecycle Flow

```text
   ┌─────────────────────────────────────────────────────────────┐
   │                    Active Session Sources                   │
   │  • Hermes Scratch: ~/.hermes/cache/scratch/hermes_agy_*     │
   │  • Standalone AGY: ~/.gemini/antigravity-cli/brain          │
   └──────────────────────────────┬──────────────────────────────┘
                                  │ (Kernel inotify trigger)
                                  ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                     Immediate Hardlink                      │
   │  ~/.agy_dashboard/archive/brain/<uuid>/.system_generated/.. │
   │  (Shares physical disk Inode — 0-copy, streams live writes) │
   └──────────────────────────────┬──────────────────────────────┘
                                  │
                 [Hermes exits or session becomes inactive]
                                  │
                                  ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                Trigger Database Ingestion                   │
   │  Executes agy_dashboard.py to parse and persist metrics     │
   │  Stores tokens, costs, models, and tools into SQLite DB     │
   └──────────────────────────────┬──────────────────────────────┘
                                  │
                 [Confirmed present in SQLite database]
                                  │
                                  ▼
   ┌─────────────────────────────────────────────────────────────┐
   │               Zip Compression & Storage Cleanup             │
   │  1. Appends raw JSONL files into transcripts_archive.zip    │
   │  2. Unlinks uncompressed JSONL files from staging           │
   │  3. Frees disk space (~85-90% compression ratio)            │
   └─────────────────────────────────────────────────────────────┘
```

---

## How It Works in Detail

### 1. Inode-Level Hardlinking (Immediate Capture)
Waiting for a session to end before capturing logs is too late because Hermes deletes the folder upon exit. The archiver uses Linux **filesystem hardlinks** (`os.link`):

1. The moment Hermes begins a turn, `transcript.jsonl` is created on disk (e.g., Inode `#4819234`).
2. The archiver immediately creates a hardlink pointing to that exact same Inode under `~/.agy_dashboard/archive/brain/<uuid>/`.
3. **Live writes are shared**: As Hermes streams messages and tokens, the writes update the shared Inode in real time with zero CPU or disk copying overhead.
4. **Survival on exit**: When Hermes terminates and unlinks its scratch directory, the file's link count drops from 2 to 1. **The physical file content remains intact on disk in `~/.agy_dashboard/`**.

### 2. Discerning Sources & Preventing Double Counting
Sessions originating from different paths are tagged by origin:
* `hermes`: Sourced from `~/.hermes/cache/scratch/` or `~/.hermes/profiles/`.
* `agy_standalone`: Sourced from `~/.gemini/antigravity-cli/brain/`.

**Deduplication Guarantee**:
Each Antigravity session generates a globally unique UUID (`conversation_id`). The SQLite schema uses `id` as the primary key. If a session is captured from Hermes scratch and later scanned elsewhere, SQLite performs an `INSERT OR REPLACE` (upserting with the highest byte watermark and turn count). Tokens are never counted twice.

### 3. Session Inactivity & Completion Detection
A conversation in staging is marked ready for indexing and archiving when:
* **Hermes Exited**: The originating scratch directory under `~/.hermes/cache/scratch/` no longer exists on disk.
* **Inactivity Threshold**: For standalone AGY sessions or surviving sessions, no new bytes have been written for `inactivity_threshold` seconds (default: 60s).
* **Manual Sync**: Running `agy_archiver.py --once` immediately evaluates all current transcripts.

### 4. Database Ingestion (Triggering Parser)
Before any file is touched, the archiver runs `agy_dashboard.py` in summary/index mode. The parser reads the staged transcripts and commits all usage data to SQLite (`~/.agy_dashboard/<hostname>.db`):
* Turn-by-turn input tokens, context window tokens, output tokens, and thinking tokens
* Model breakdowns (Gemini 2.5 Pro/Flash, Claude Sonnet/Opus, etc.)
* API request counts and subagent spawn counts
* Tool invocations
* Carbon emissions, electricity (Wh), and cooling water calculations

**Integrity Guard**: The archiver verifies that `SELECT id FROM conversations WHERE id = ?` returns true in SQLite before proceeding to the compression and deletion step. If a file fails to parse, it is never deleted.

### 5. Storage Optimization: ZIP Compression & Uncompressed Cleanup
Raw Antigravity `transcript.jsonl` files are verbose JSON documents that consume tens or hundreds of megabytes over weeks of usage.

1. **ZIP Deflate Compression**: All verified conversations are added to `~/.agy_dashboard/transcripts_archive.zip` using `ZIP_DEFLATED`. This yields an **85% to 90% space reduction** (e.g., 88 MB of raw transcripts compresses to ~10 MB).
2. **Uncompressed Deletion**: Once safely written to the ZIP file, the uncompressed `.jsonl` files in the staging directory are deleted (`os.unlink()`), and empty directory trees are pruned.
3. **Preserved Internal Hierarchy**: Inside `transcripts_archive.zip`, files retain their relative structure:
   ```text
   transcripts_archive.zip
   └── brain/<conversation_uuid>/.system_generated/logs/transcript.jsonl
   ```

---

## Terminal & Web Dashboard Display

Both the CLI summary and the web dashboard automatically inspect `~/.agy_dashboard/transcripts_archive.zip` and display its footprint in real time.

### Terminal Summary (`agy_dashboard.py --summary`)
```text
══════════════════════════════════════════════════════════════════════════════
  AGY CLI USAGE TRACKER  •  LIVE DATA  •  All time
══════════════════════════════════════════════════════════════════════════════
  Brain Dir    : /workspace/.gemini/antigravity-cli/brain
  Zip Archive  : 10.5 MB  (1004 files in transcripts_archive.zip)
  Billing Mode : API Context Tokens (OpenRouter-style)
  Pricing Tier : AGY Blended ($0.50 / $2.50 per 1M tok)
──────────────────────────────────────────────────────────────────────────────
  Total Tokens         : 26,107,563 (26.1M)
  Effective Input      : 25,322,968
    ├─ Context Input   : 24,766,808
    └─ Unique Input    : 556,160
  Output Tokens        : 784,595
  Thinking Tokens      : 3,966,547 (15.2%)
  API Requests         : 537  (avg 48,617 tok/req)
  Conversations        : 509  (6 active days)
──────────────────────────────────────────────────────────────────────────────
```

### Web Dashboard
In the top status bar (alongside DB path and last scan timestamp), the `zip:` pill displays the current archive size (e.g., `zip: 10.5 MB`).

### JSON API (`/api/stats`)
The metadata block includes:
```json
{
  "meta": {
    "archive_zip_path": "/workspace/.agy_dashboard/transcripts_archive.zip",
    "archive_zip_size": "10.5 MB",
    "archive_zip_bytes": 11002047,
    "archive_zip_exists": true,
    "archive_zip_count": 1004
  }
}
```

---
## Safe Deletion & Backwards Compatibility

> [!IMPORTANT]
> ### Can the ZIP Archive and Raw Transcripts Be Deleted?
> **YES, but understand the trade-off:**
>
> 1. **Nothing in the dashboard is lost if you delete the ZIP file:**
>    * All historical metrics—total token counts, context/input/output/thinking breakdowns, per-model usage, costs, API request counts, tool executions, daily peaks, electricity (Wh), and carbon footprint—are stored **permanently** inside the SQLite database (`~/.agy_dashboard/<hostname>.db`).
>    * Even if you completely delete `transcripts_archive.zip` and the `archive/` folder, your dashboard will continue showing all historical token charts and statistics without losing a single token.
>
> 2. **What is lost (Backwards Compatibility for Future Features):**
>    * **Raw prompt and response text**: The SQLite database only indexes metadata, turn metrics, and tool statistics; it does NOT store the full multi-megabyte message bodies to conserve disk space.
>    * **Re-parsing capability**: If a future version of `agy_dashboard.py` adds support for a brand-new metric (e.g., tracking specific tool argument patterns, subagent conversation trees, or newly formatted telemetry fields from future Gemini/AGY releases), that new metric can only be retroactively extracted for past sessions if the raw transcripts in the ZIP archive exist to be re-parsed.

---

## Power & Battery Optimization: Zero-Polling via Linux `inotify`

Traditional background monitors run `while True: sleep(3)` loops, which wake the CPU several times per minute, preventing Linux from dropping into deep C-states (power sleep) and draining laptop battery.

`agy_archiver.py` uses the **Linux `inotify` kernel subsystem** directly via standard library `ctypes` (`libc.so.6`):
* While idling, the archiver thread enters **uninterruptible kernel sleep** waiting on `select()` on the inotify file descriptor.
* It consumes **0.00% CPU** and **zero battery**.
* When Hermes or AGY creates or modifies a file, the Linux kernel wakes the archiver process instantaneously.

---

## Installation & Service Setup

### Option A: Automated Systemd Installer (Recommended)
Run the automated installer script, which detects paths, configures the unit file, enables, and starts the service:

```bash
./install.sh
```

To check status or manage the service later:
```bash
./install.sh --status        # View systemctl --user status
./install.sh --uninstall     # Stop, disable, and clean up service unit
```

### Option B: Manual Systemd Setup
A ready-to-use user service definition is provided in `agy-archiver.service`:

```bash
# 1. Copy service file to user systemd directory
mkdir -p ~/.config/systemd/user
cp agy-archiver.service ~/.config/systemd/user/agy-archiver.service

# 2. Reload and enable the service
systemctl --user daemon-reload
systemctl --user enable --now agy-archiver.service

# 3. Check status
systemctl --user status agy-archiver.service
```

### Option C: Manual CLI Execution

```bash
# Check status of staging, database, and zip archive
./agy_archiver.py --status

# Run a single sweep, index, zip, and cleanup pass
./agy_archiver.py --once

# Run in foreground with verbose diagnostic output
./agy_archiver.py -v
```

### CLI Options Reference

| Flag | Default | Description |
|---|---|---|
| `--once` | `False` | Run one complete sweep, index, zip, and cleanup cycle, then exit |
| `--status` | `False` | Print current storage metrics, SQLite count, and ZIP size |
| `--interval` | `3.0` | Inotify wait in kernel select (zero CPU wakeups when idle) |
| `--inactivity-threshold` | `60.0` | Inactive seconds before a session is zipped and cleaned |
| `--index-interval` | `120.0` | Seconds between background SQLite indexing sweeps |
| `--dashboard-dir` | `~/.agy_dashboard` | Target storage directory |
| `--zip-name` | `transcripts_archive.zip` | Name of the compressed archive file |
| `--no-zip` | `False` | Disable zipping (leaves uncompressed files in staging) |
| `--dry-run` | `False` | Simulate all operations without modifying disk or DB |
| `-v`, `--verbose` | `False` | Print verbose operational logs |

---

## Multi-Device Synchronization & Deduplication

- **Conversation UUID Deduplication**: Sessions across devices are keyed by UUID (`conversations.id`).
- **Event Step Protection**: A unique index on `(conversation_id, step_index)` guarantees that merging multi-device databases never duplicates tokens or tool events.
- **Origin Attribution**: Telemetry retains device provenance (`events.device` and `conversations.device`), allowing filtering across all systems or by individual machine in both the Web UI and CLI.
