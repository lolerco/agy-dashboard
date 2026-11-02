# Cross-Device Telemetry Synchronization Proposal

This document outlines the architectural proposal for synchronizing Google Antigravity (`agy`) telemetry databases across multiple machines (e.g. desktop workstation, laptop, remote server) into a unified usage tracking view.

---

## 1. Core Principles & Constraints

1. **Zero External Dependencies (Stdlib-Only):** Any syncing logic built into `agy_dashboard.py` must run on Python 3.9+ standard library without requiring external packages or complex daemons.
2. **Collision-Free Data Model:** Antigravity generates version 4 UUIDs for all conversation IDs (e.g. `0dc1f693-fd59-46c3-a518-90789b3530b4`). Sessions across separate devices will never have conflicting primary keys.
3. **Idempotency:** Re-syncing the same file or date range multiple times must produce identical results without duplicating tokens or events.

---

## 2. Schema Evolution: Device Identification

To distinguish data origins and support multi-device analytics, we add `device_id` and `device_name` columns to our SQLite index:

```sql
ALTER TABLE conversations ADD COLUMN device_id TEXT NOT NULL DEFAULT 'local';
ALTER TABLE events ADD COLUMN device_id TEXT NOT NULL DEFAULT 'local';
ALTER TABLE tool_usages ADD COLUMN device_id TEXT NOT NULL DEFAULT 'local';
```

### Auto-Detection & Custom Overrides
- **Default `device_id`:** Resolved via `socket.gethostname().lower().strip()` or configured via CLI flag `--device <id>`.
- **Human-Readable Labels:** A config dictionary or CLI argument mapping device IDs to UI friendly names (e.g., `desktop-cachyos` → "Desktop Workstation", `thinkpad-x1` → "Laptop").

---

## 3. Synchronization Strategies

### Option A: Direct Peer-to-Peer HTTP Sync (Recommended)
Since `agy_dashboard.py` already includes a built-in multi-threaded HTTP server, we can expose lightweight REST endpoints:

1. **Endpoints:**
   - `POST /api/sync/push`: Accepts a batch of JSON records from another device.
   - `GET /api/sync/pull?since=<timestamp>`: Returns incremental records updated since the specified timestamp.
   - Optional `--sync-secret <token>` header to prevent unauthorized access on local networks.

2. **Client CLI Command:**
   ```bash
   # Push local telemetry to the primary workstation
   python3 agy_dashboard.py --sync-to http://desktop.local:8765 --sync-secret mysecret
   ```
   Implemented entirely with `urllib.request` and standard JSON serialization.

---

### Option B: Shared Folder / Cloud Drive Export (Offline-First)
For setups where devices are not online at the same time or use tools like Syncthing, Nextcloud, Dropbox, or Git-annex:

1. **Export Command:**
   ```bash
   python3 agy_dashboard.py --export-device-sync ~/Sync/agy-telemetry/
   ```
   Writes an incremental JSONL or SQLite partition: `~/Sync/agy-telemetry/<device_id>.agy.db`.

2. **Auto-Ingestion:**
   When launching the dashboard with `--sync-dir ~/Sync/agy-telemetry/`, the scanner merges all remote device databases into the primary local cache.

---

### Option C: Multi-Path / Remote Brain Scanning
Mount remote brain directories via SSHFS or NFS and point the scanner at multiple paths:
```bash
python3 agy_dashboard.py --brain-dir local=~/.gemini/antigravity-cli/brain --brain-dir laptop=/mnt/laptop/brain
```

---

## 4. UI & Dashboard Analytics Experience

1. **Top Bar Device Selector:**
   A filter dropdown in the top action bar:
   ```
   [🖥️ All Devices (3) ▾]  [Live 10s]  [Rescan]  [CSV]  [📖 Glossar]
     ├─ ● All Devices (Combined)
     ├─ 💻 desktop-workstation (1.8M tok • 98 reqs)
     ├─ 📱 thinkpad-laptop   (720K tok • 36 reqs)
     └─ ☁️ vps-cloud-runner  (216K tok • 8 reqs)
   ```

2. **Hour-of-Day Multi-Device Comparison:**
   - View the 24-hour activity distribution per device.
   - Stacked histogram bars showing which device was active at specific hours of the day.
   - Multi-day productivity averages across devices.

3. **Session List Tagging:**
   - Every session row displays a color-coded Gruvbox badge indicating the originating device.
