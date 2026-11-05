#!/usr/bin/env bash
# =============================================================================
# AGY Usage Tracker — Archiver Daemon Systemd Installer
# =============================================================================
# Sets up and starts the agy-archiver background systemd user service.
# Automatically detects project paths, installs the unit file, reloads systemd,
# and enables & starts the service with zero manual configuration.
# =============================================================================

set -euo pipefail

# Color helpers
BOLD="\033[1m"
GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
RED="\033[31m"
RESET="\033[0m"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCHIVER_SCRIPT="${PROJECT_DIR}/agy_archiver.py"
SERVICE_NAME="agy-archiver.service"

# Determine systemd user directory
USER_CONFIG_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}"
SYSTEMD_USER_DIR="${USER_CONFIG_DIR}/systemd/user"
TARGET_SERVICE="${SYSTEMD_USER_DIR}/${SERVICE_NAME}"
WANTS_DIR="${SYSTEMD_USER_DIR}/default.target.wants"
TARGET_WANTS="${WANTS_DIR}/${SERVICE_NAME}"

print_banner() {
    echo -e "${BOLD}${BLUE}╔══════════════════════════════════════════════════════════════╗${RESET}"
    echo -e "${BOLD}${BLUE}║     AGY & Hermes Transcript Archiver — Service Installer     ║${RESET}"
    echo -e "${BOLD}${BLUE}╚══════════════════════════════════════════════════════════════╝${RESET}"
}

uninstall_service() {
    echo -e "${YELLOW}Uninstalling ${SERVICE_NAME}...${RESET}"
    if command -v systemctl >/dev/null 2>&1; then
        systemctl --user stop "${SERVICE_NAME}" 2>/dev/null || true
        systemctl --user disable "${SERVICE_NAME}" 2>/dev/null || true
    fi
    rm -f "${TARGET_SERVICE}" "${TARGET_WANTS}"
    if command -v systemctl >/dev/null 2>&1; then
        systemctl --user daemon-reload 2>/dev/null || true
    fi
    echo -e "${GREEN}✓ Successfully removed ${SERVICE_NAME}.${RESET}"
    exit 0
}

check_status() {
    echo -e "${BOLD}Checking ${SERVICE_NAME} status:${RESET}"
    if command -v systemctl >/dev/null 2>&1; then
        systemctl --user status "${SERVICE_NAME}" || true
    else
        echo -e "${RED}systemctl is not available in PATH.${RESET}"
    fi
    exit 0
}

# Handle flags
case "${1:-}" in
    --uninstall|-u)
        uninstall_service
        ;;
    --status|-s)
        check_status
        ;;
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  (no args)       Install, enable, and start agy-archiver systemd service"
        echo "  --status, -s    Check service status via systemctl --user status"
        echo "  --uninstall, -u Stop, disable, and remove the systemd service"
        echo "  --help, -h      Display this help dialog"
        exit 0
        ;;
esac

print_banner

# 1. Verify python environment
echo -e "${BOLD}[1/5] Checking Python environment...${RESET}"
PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [[ -z "${PYTHON_BIN}" ]]; then
    echo -e "${RED}Error: Python 3 was not found in PATH. Please install Python 3.9+.${RESET}"
    exit 1
fi

PY_VER="$("${PYTHON_BIN}" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || echo "0.0")"
PY_OK="$("${PYTHON_BIN}" -c 'import sys; print(int(sys.version_info >= (3, 9)))' 2>/dev/null || echo "0")"

if [[ "${PY_OK}" != "1" ]]; then
    echo -e "${RED}Error: Python 3.9+ is required (found Python ${PY_VER}).${RESET}"
    exit 1
fi
echo -e "  ${GREEN}✓${RESET} Found Python ${PY_VER} (${PYTHON_BIN})"

# 2. Verify agy_archiver.py exists and set permissions
echo -e "${BOLD}[2/5] Checking archiver script...${RESET}"
if [[ ! -f "${ARCHIVER_SCRIPT}" ]]; then
    echo -e "${RED}Error: archiver script not found at ${ARCHIVER_SCRIPT}${RESET}"
    exit 1
fi
chmod +x "${ARCHIVER_SCRIPT}"
echo -e "  ${GREEN}✓${RESET} Archiver script verified at ${ARCHIVER_SCRIPT}"

# 3. Create systemd user directories and generate unit file
echo -e "${BOLD}[3/5] Installing systemd unit to ${SYSTEMD_USER_DIR}...${RESET}"
mkdir -p "${SYSTEMD_USER_DIR}"
mkdir -p "${WANTS_DIR}"

cat > "${TARGET_SERVICE}" <<EOF
[Unit]
Description=AGY & Hermes Transcript Archiver Daemon
Documentation=https://github.com/jakob/agy-usage-tracker
After=default.target

[Service]
Type=simple
ExecStart=${PYTHON_BIN} ${ARCHIVER_SCRIPT} --interval 3.0 --inactivity-threshold 60.0 --index-interval 120.0
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
WorkingDirectory=${PROJECT_DIR}

# Power and battery efficiency: low IO and CPU scheduling
Nice=10
IOSchedulingClass=best-effort
IOSchedulingPriority=6

[Install]
WantedBy=default.target
EOF

# Also keep repo template synced
cp "${TARGET_SERVICE}" "${PROJECT_DIR}/agy-archiver.service"
echo -e "  ${GREEN}✓${RESET} Created ${TARGET_SERVICE}"

# 4. Enable and start service via systemctl
echo -e "${BOLD}[4/5] Enabling and starting service...${RESET}"
CAN_CONNECT_SYSTEMD=0
if command -v systemctl >/dev/null 2>&1; then
    if systemctl --user daemon-reload >/dev/null 2>&1; then
        CAN_CONNECT_SYSTEMD=1
    fi
fi

if [[ "${CAN_CONNECT_SYSTEMD}" -eq 1 ]]; then
    systemctl --user enable "${SERVICE_NAME}"
    systemctl --user restart "${SERVICE_NAME}"
    echo -e "  ${GREEN}✓${RESET} Service enabled and started via systemctl --user"
else
    # Fallback for containers or shells without active user DBus socket
    ln -sf "${TARGET_SERVICE}" "${TARGET_WANTS}"
    echo -e "  ${YELLOW}!${RESET} Active systemd user bus was not reachable in this session (common in Docker or subshells)."
    echo -e "  ${GREEN}✓${RESET} Automatically created enablement symlink in ${TARGET_WANTS}"
    echo -e "  ${BLUE}ℹ${RESET} To activate on host, run: ${BOLD}systemctl --user daemon-reload && systemctl --user start ${SERVICE_NAME}${RESET}"
fi

# 5. Run initial sync verification pass
echo -e "${BOLD}[5/5] Running initial synchronization pass...${RESET}"
"${PYTHON_BIN}" "${ARCHIVER_SCRIPT}" --once || true

echo ""
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${GREEN}Installation Complete!${RESET}"
echo -e "The archiver is now configured to watch for Hermes & AGY transcripts."
echo -e "• Service File  : ${TARGET_SERVICE}"
echo -e "• Check Status  : ${BOLD}systemctl --user status ${SERVICE_NAME}${RESET} (or ${BOLD}$0 --status${RESET})"
echo -e "• View Logs     : ${BOLD}journalctl --user -u ${SERVICE_NAME} -f${RESET}"
echo -e "• Uninstall     : ${BOLD}$0 --uninstall${RESET}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════════════${RESET}"
