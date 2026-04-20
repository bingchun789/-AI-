#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-status}"
SERVICES=(ai-select-bot.service ai-select-dashboard.service ai-select-monitor.service)

existing_services=()
for service in "${SERVICES[@]}"; do
  if systemctl list-unit-files "$service" >/dev/null 2>&1; then
    existing_services+=("$service")
  fi
done

if [[ ${#existing_services[@]} -eq 0 ]]; then
  echo "No installed ai-select services found."
  exit 1
fi

case "$ACTION" in
  start)
    systemctl start "${existing_services[@]}"
    ;;
  stop)
    systemctl stop "${existing_services[@]}"
    ;;
  restart)
    systemctl restart "${existing_services[@]}"
    ;;
  status)
    for service in "${existing_services[@]}"; do
      systemctl status "$service" --no-pager || true
      echo
    done
    ;;
  logs)
    journalctl $(printf -- "-u %s " "${existing_services[@]}") -n 100 --no-pager
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs}"
    exit 1
    ;;
esac
