# AGY Usage Tracker (`agy_dashboard.py`)

Zero-dependency, single-file telemetry tracker and Linear-styled local web dashboard for **Google Antigravity CLI (`agy`)** — whether you run `agy` standalone, inside the Antigravity IDE, or as a headless inference backend for **Hermes Agent**.

Requires only **Python 3.9+** (`stdlib` only — zero `pip install`, zero external packages, 100% offline).

---

## Why It Exists

When running long agentic coding loops or routing Hermes Agent sessions through `agy`, local sessions generate rich `.jsonl` transcripts on disk without built-in spend or token visibility. `agy_dashboard.py` turns those local logs into **OpenRouter-style analytics** on `localhost`:

- **Daily & Total Token Volume** — Input, output, reasoning (`thinking`), and tool-call token breakdowns over time.
- **Context vs. Unique Turn Billing Toggle** — Switch instantly between cumulative API-billed context window tokens and net-new per-turn tokens.
- **API Request & Step Telemetry** — Total model invocations (`PLANNER_RESPONSE` steps), turns per conversation, and error rates.
- **Theoretical API Dollar Value** — Estimate equivalent pay-per-token API cost across selectable model pricing tiers (Flash, Standard, Pro / Frontier).
- **Electricity & Carbon Footprint** — Estimated datacenter inference energy (**Wh / kWh**) and emissions (**g/kg CO₂e**).
- **365-Day Activity Heatmap** — Year-long daily intensity grid to spot heavy coding streaks.
- **Hourly Usage Profile** — 24-hour distribution showing peak agent activity by time of day.
- **Tool Usage Analytics** — Frequency and volume breakdown across native AGY and delegated Hermes tools.
- **Per-Conversation Breakdown** — Searchable, sortable table of individual sessions with token, cost, request, and timestamp details.

---

## Quick Start

No virtual environment or package installation needed. Run directly with Python 3.9+:

```bash
# Launch the local web dashboard (opens http://127.0.0.1:8765 automatically)
python3 agy_dashboard.py

# Preview the UI with realistic sample data on a fresh machine
python3 agy_dashboard.py --demo

# Print an instant terminal summary without starting a browser
python3 agy_dashboard.py --summary
```

---

## Background Archiver & Systemd Installation (`agy_archiver.py`)

If you interact with Gemini through **Hermes Agent** or run frequent Antigravity sessions, Hermes isolates each execution in a temporary scratch directory (`~/.hermes/cache/scratch/hermes_agy_*`) and **deletes it upon session termination (`/quit`)**. Without a continuous background watcher, these ephemeral sessions and their token counts are lost forever.

The repository provides **`agy_archiver.py`**, an ultra-low-power, zero-dependency background daemon that solves this non-invasively:
1. **Immediate Inode Hardlinking:** Captures new `transcript.jsonl` files within milliseconds using hardlinks, allowing transcripts to continue receiving live writes and survive Hermes unlinking its scratch directory.
2. **Zero-Polling Linux `inotify`:** Uses kernel events via standard library `ctypes` (`libc.so.6`). When idling, it consumes **0.00% CPU and zero battery**.
3. **Automatic Deduplication:** Discovers both Hermes and Standalone AGY sessions, deduplicating conversations by UUID in SQLite so no session is ever double-counted.
4. **Database Indexing & ZIP Storage Optimization:** Once a session finishes, it triggers SQLite database indexing and compresses completed transcripts into `~/.agy_dashboard/transcripts_archive.zip`, freeing raw disk space while keeping permanent token history.

### Automated 1-Step Installation (`install.sh`)

To install, configure, and start the archiver as a persistent systemd user service:

```bash
./install.sh
```

#### What `install.sh` performs:
- **Environment verification:** Confirms Python 3.9+ is installed and sets execute permissions on `agy_archiver.py`.
- **Dynamic service configuration:** Generates `~/.config/systemd/user/agy-archiver.service` tailored to your machine's exact Python executable and project directory path.
- **Power & priority optimization:** Configures `Nice=10` and `IOSchedulingClass=best-effort` for unobtrusive background operation.
- **Service enablement & startup:** Runs `systemctl --user daemon-reload` and `systemctl --user enable --now agy-archiver.service`. (In containers or shells lacking an active D-Bus session bus, it automatically links into `default.target.wants` and outputs host startup commands).
- **Initial sync verification:** Runs an immediate synchronization pass (`agy_archiver.py --once`) to ingest existing transcripts into SQLite.

### Managing the Archiver Service

| Action | Command |
| :--- | :--- |
| **Check service status** | `./install.sh --status` *(or `systemctl --user status agy-archiver.service`)* |
| **View live logs** | `journalctl --user -u agy-archiver.service -f` |
| **Restart service** | `systemctl --user restart agy-archiver.service` |
| **Stop & uninstall** | `./install.sh --uninstall` |
| **Run single sync pass** | `python3 agy_archiver.py --once` |
| **Inspect archive stats** | `python3 agy_archiver.py --status` |

> 📖 **Deep Dive Documentation:** For full architectural diagrams, hardlink mechanics, double-count prevention, and ZIP retention details, see [ARCHIVER.md](ARCHIVER.md).


---

## CLI Options Reference

| Option | Default | Description |
| :--- | :--- | :--- |
| `--port <PORT>` | `8765` | Port for the local HTTP dashboard server. |
| `--host <HOST>` | `127.0.0.1` | Bind address for the HTTP server (use `127.0.0.1` to keep strictly local). |
| `--agy-dir <PATH>` | Auto-detected | Path to the AGY `brain` directory containing conversation folders (may be passed to scan custom or Hermes scratch workspaces). |
| `--db <PATH>` | Local cache file | Path to the SQLite incremental cache file used to store parsed transcript metrics. |
| `--no-browser` | `False` | Start the web server without automatically opening your default browser. |
| `--demo` | `False` | Populate the dashboard (or `--summary`) with realistic synthetic telemetry data — useful for testing or previewing without local AGY logs. |
| `--prune` | `False` | Prune records from SQLite if transcript files no longer exist on disk (default is false: preserves all history as a permanent archive). |
| `--summary` | `False` | Print a concise telemetry report directly in the terminal and exit immediately. |
| `--export-json <PATH>` | None | Export aggregated metrics and conversation summaries to a JSON file and exit. |

### Useful Examples

```bash
# Run headless on a remote devbox and forward port 9000 over SSH
python3 agy_dashboard.py --host 127.0.0.1 --port 9000 --no-browser

# Point at a custom brain directory and export JSON metrics
python3 agy_dashboard.py --agy-dir /path/to/.gemini/antigravity-cli/brain --export-json usage.json
```

---

## Sharing with Friends (Linux, macOS & Windows)

Because the entire backend, SQLite indexer, CLI reporter, and Linear-styled web frontend live inside **one self-contained `.py` file**, sharing takes seconds:

1. Send or copy **`agy_dashboard.py`** (single file — nothing else required).
2. Ensure **Python 3.9+** is installed (`python3 --version` or `python --version`).
3. Run it — `agy_dashboard.py` automatically locates the default Antigravity transcript directory for each OS:

| Platform | Default Transcript Directory (`brain`) | Run Command |
| :--- | :--- | :--- |
| **Linux** | `~/.gemini/antigravity-cli/brain` | `python3 agy_dashboard.py` |
| **macOS** | `~/.gemini/antigravity-cli/brain` | `python3 agy_dashboard.py` |
| **Windows** | `%USERPROFILE%\.gemini\antigravity-cli\brain` | `python agy_dashboard.py` |

> **Privacy First:** Everything runs locally on `127.0.0.1`. Conversation transcripts, prompts, code snippets, and SQLite caches never leave your machine.

If a friend uses Hermes Agent with isolated scratch home directories (e.g., `~/.hermes/cache/scratch/.../home/.gemini/antigravity-cli/brain`), pass `--agy-dir` pointing to that path.

---

## How Metrics Are Calculated

### 1. Transcript Parsing & `transcript_full.jsonl` Truncation Resolution
Each AGY conversation stores execution steps under:
```text
<brain>/<conversation-id>/.system_generated/logs/transcript.jsonl
```
To keep active logs compact, AGY truncates large step payloads in `transcript.jsonl` and marks the affected keys in `truncated_fields` (e.g., `content`, `thinking`, `tool_calls`). `agy_dashboard.py` reads the fast compact `transcript.jsonl` stream first and **selectively resolves truncated line indices from `transcript_full.jsonl`** when present. This preserves exact character-to-token estimation on huge tool outputs and reasoning traces without needlessly re-reading gigabytes of un-truncated logs.

### 2. Billed Context Tokens vs. Unique Turn Tokens
Agentic workflows re-send prior conversation history on every model turn, so raw new text and billed API tokens differ substantially:
- **Billed Context Tokens (API-Equivalent):** For every `PLANNER_RESPONSE` request, accumulates all prior conversation context (user prompts, tool results, prior model responses) as **input context tokens**, while counting the step's generated response, `thinking` trace, and emitted `tool_calls` as **output tokens**. This mirrors how commercial LLM APIs meter multi-turn agent loops.
- **Unique Turn Tokens:** Counts each message, tool payload, reasoning block, and model output **once** when created, without compounding across subsequent turns. Ideal for measuring net information produced and ingested.

You can toggle between **Billed Context** and **Unique Turn** views live in the dashboard.

### 3. Customizable Model Pricing Tiers
Because local AGY CLI sessions don't embed dollar invoices, the dashboard computes **theoretical API value** by applying per-1M-token rates (input, context/cached input, and output + thinking tokens) across selectable reference model tiers (e.g., lightweight Flash, balanced Standard/Sonnet-class, and Frontier/Pro reasoning tiers) or custom rates directly in the UI.

### 4. Datacenter TPU/GPU Energy & Grid CO₂ Benchmarks
Environmental estimates model active LLM inference physics rather than flat per-request guesses:
- **Energy (Wh / kWh):** Separates lower-cost parallel **prefill** (input tokens) from memory-bandwidth-bound autoregressive **decode** (output + thinking tokens) using published datacenter TPU/GPU Wh-per-1k-token benchmarks, scaled by datacenter **PUE (Power Usage Effectiveness)**.
- **Carbon Footprint (gCO₂e / kgCO₂e):** Multiplies estimated kWh consumption by standard cloud/grid carbon intensity (`gCO₂e/kWh`) to surface practical equivalents.

### 5. Incremental SQLite Caching & Multi-Device Synchronization
Parsing thousands of `.jsonl` agent trajectories on every page load is slow. `agy_dashboard.py` uses Python's built-in `sqlite3` module to persist computed per-conversation, daily, hourly, and tool statistics keyed by transcript path, modification timestamp (`mtime`), and file size. Unchanged sessions load instantaneously from SQLite; only newly created or actively growing transcripts are re-indexed.

#### Multi-Device Syncing with Syncthing (or Git / rsync)
Each machine maintains its own isolated database under `~/.agy_dashboard/<device_name>.db` (defaulting to the host machine's hostname). When syncing across multiple devices, `agy_dashboard.py` automatically discovers all `*.db` files in `~/.agy_dashboard/` and unifies them in-memory, allowing cross-device stats, filtering, and badging without database file locking conflicts.

**Setting up with Syncthing:**
1. Sync `~/.agy_dashboard/` across your machines.
2. In Syncthing's folder ignore patterns (`.stignore`), exclude active SQLite WAL/temporary lock files:
   ```text
   *.db-wal
   *.db-shm
   *.db-journal
   ```
3. Each machine will read and write to its own `<device_name>.db` while receiving peer `<other_device>.db` files in the background.

**Using Other Syncing Mechanisms (Git, rsync, cloud storage):**
- If syncing via Git or backup scripts, ensure only the main `*.db` files are committed or copied (ignore `*.db-wal` and `*.db-shm`).
- Because database filenames are unique per machine (`laptop.db`, `desktop.db`, `server.db`), there are no merge conflicts or concurrent SQLite lock contentions.
