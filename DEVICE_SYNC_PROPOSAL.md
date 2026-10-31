# Multi-Device Database Synchronization Proposal

This document outlines the architecture, data models, and synchronization strategies for synchronizing Google Antigravity (`agy`) telemetry databases across multiple developer machines (e.g., workstation, laptop, remote server) into a unified dashboard view.

---

## 1. Zero-Collision Data Model

Google Antigravity CLI assigns unique UUIDs to every session folder (e.g. `~/.gemini/antigravity-cli/brain/0dc1f693-fd59-46c3-a518-90789b3530b4`). Because conversation IDs are globally unique UUIDs, sessions generated across distinct devices will never collide.

### Schema Changes

Add device tagging across all primary telemetry tables:

```sql
ALTER TABLE conversations ADD COLUMN device_id TEXT NOT NULL DEFAULT 'local';
ALTER TABLE events ADD COLUMN device_id TEXT NOT NULL DEFAULT 'local';
ALTER TABLE tool_usages ADD COLUMN device_id TEXT NOT NULL DEFAULT 'local';

CREATE INDEX IF NOT EXISTS idx_events_device ON events(device_id);
CREATE INDEX IF NOT EXISTS idx_conv_device ON conversations(device_id);
```

### Device Identification

- **Automatic fallback**: `socket.gethostname()`
- **CLI override**: `--device <name>` (e.g. `agy_dashboard.py --device thinkpad-arch`)
- **Friendly Metadata**: Optional alias and UI color tag (e.g. `Desktop Workstation` [Gruvbox Green], `MacBook Air` [Gruvbox Blue], `Hetzner Cloud` [Gruvbox Orange]).

---

## 2. Synchronization Mechanisms

### Option A: Direct Peer-to-Peer HTTP Sync (Recommended — 100% Stdlib)

Because `agy_dashboard.py` already includes a lightweight Python `http.server`, it can serve both web UI requests and peer-to-peer sync requests without third-party dependencies.

1. **Primary/Host Device** (e.g. desktop workstation hosting the dashboard at `http://desktop.local:8765`):
   - Exposes `/api/sync/push` and `/api/sync/pull`.
   - Optional authentication token flag: `--sync-key <secret>`.
2. **Secondary/Client Device** (e.g. laptop after working remotely):
   - Runs a single sync command:
     ```bash
     python3 agy_dashboard.py --sync-to http://desktop.local:8765 [--sync-key <secret>]
     ```
   - Computes incremental records where `updated_at > last_sync_time`.
   - Client transmits telemetry payload via `urllib.request`.
   - Receiver performs `INSERT OR REPLACE INTO conversations ...`, keeping both datasets up to date.

### Option B: Shared Directory / File-Drop Sync (Syncthing, Dropbox, Git, Network Share)

Ideal for asynchronous or offline-first setups where devices are not online simultaneously:

1. Each client periodically exports incremental JSONL/SQLite dumps to a designated sync folder:
   `~/Sync/agy-telemetry/<device_id>.jsonl`
2. The dashboard monitors or scans the sync folder:
   ```bash
   python3 agy_dashboard.py --sync-dir ~/Sync/agy-telemetry/
   ```
3. On rescan or launch, files from all devices are ingested into the unified SQLite cache.

### Option C: Multi-Path Brain Directory Mounting

Point the scanner to multiple local or mounted directories directly:

```bash
python3 agy_dashboard.py \
  --brain-dir local=~/.gemini/antigravity-cli/brain \
  --brain-dir laptop=/mnt/laptop-brain/brain
```

---

## 3. UI & Analytics Integration

### Top Bar Device Switcher

Add a segmented control or dropdown in the top controls bar next to the live-polling and rescan actions:

```text
[🖥️ All Devices (3) ▾]  [Live 10s]  [Rescan]  [CSV]  [dark]  [📖 Glossar]
  ├─ ● All Devices (Combined: 2.74M tok • 142 reqs)
  ├─ 💻 desktop-cachyos (1.81M tok • 98 reqs)
  ├─ 📱 laptop-thinkpad (718K tok • 36 reqs)
  └─ ☁️ vps-remote     (216K tok • 8 reqs)
```

### Multi-Device Activity Analysis

1. **Device-Aware Hour-of-Day Histogram**:
   - Stacked bars indicating which hours and devices were most active across days.
   - Multi-day averages comparing developer routines across machines.
2. **Session Badging**:
   - Distinct Gruvbox color badges per device in the **Recent Conversations** table.
3. **Filtering**:
   - Switching devices in the UI instantly filters KPI cards, token distribution charts, and session tables without a full page reload.
