#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${1:-$(cd "$(dirname "$0")" && pwd)}"
SERVICE_USER="${2:-root}"
PORT="${3:-8787}"

if [[ ! -d "$PROJECT_DIR" ]]; then
  echo "Project directory not found: $PROJECT_DIR"
  exit 1
fi

if [[ ! -f "$PROJECT_DIR/.env" ]]; then
  echo ".env not found in: $PROJECT_DIR"
  exit 1
fi

if [[ -x "$PROJECT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
else
  PYTHON_BIN="$(command -v python3)"
fi

BOT_SERVICE_PATH="/etc/systemd/system/ai-select-bot.service"
DASHBOARD_SERVICE_PATH="/etc/systemd/system/ai-select-dashboard.service"
MONITOR_SERVICE_PATH="/etc/systemd/system/ai-select-monitor.service"

cat > "$BOT_SERVICE_PATH" <<EOF
[Unit]
Description=AI Select Binance Bot Loop
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
WorkingDirectory=${PROJECT_DIR}
ExecStart=${PYTHON_BIN} ${PROJECT_DIR}/ai_select_futures_bot.py --loop
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

cat > "$DASHBOARD_SERVICE_PATH" <<EOF
[Unit]
Description=AI Select Dashboard
After=network-online.target ai-select-bot.service ai-select-monitor.service
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
WorkingDirectory=${PROJECT_DIR}
ExecStart=${PYTHON_BIN} ${PROJECT_DIR}/dashboard.py --no-browser --host 0.0.0.0 --port ${PORT}
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1
Environment=DASHBOARD_HOST=0.0.0.0

[Install]
WantedBy=multi-user.target
EOF

cat > "$MONITOR_SERVICE_PATH" <<EOF
[Unit]
Description=AI Select Monitor
After=network-online.target ai-select-bot.service
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
WorkingDirectory=${PROJECT_DIR}
ExecStart=${PYTHON_BIN} ${PROJECT_DIR}/monitor.py --loop
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable ai-select-bot.service
systemctl enable ai-select-dashboard.service
systemctl enable ai-select-monitor.service
systemctl restart ai-select-bot.service
systemctl restart ai-select-dashboard.service
systemctl restart ai-select-monitor.service

echo "Installed services:"
echo "  ai-select-bot.service"
echo "  ai-select-dashboard.service"
echo "  ai-select-monitor.service"
echo
echo "Check status:"
echo "  systemctl status ai-select-bot.service"
echo "  systemctl status ai-select-dashboard.service"
echo "  systemctl status ai-select-monitor.service"
echo
echo "If you use UFW, allow the dashboard port:"
echo "  ufw allow ${PORT}/tcp"
