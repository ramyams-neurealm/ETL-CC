#!/usr/bin/env bash

set -euo pipefail

ACTION="${1:-status}"

ACTIVE_SERVICES=(
  etl-cc-api
  etl-cc-discovery
  etl-cc-orchestrator
)

LEGACY_SERVICES=(
  etl-cc-conversion
  etl-cc-validation
  etl-cc-deployment
)

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Run as root or with sudo."
    exit 1
  fi
}

install_orchestrator_unit() {
  cat > /etc/systemd/system/etl-cc-orchestrator.service <<'UNIT'
[Unit]
Description=ETL CC LangGraph Migration Orchestrator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=/home/authuser/dev/chatbots/ETL_CC
EnvironmentFile=/etc/etl-cc/etl-cc.env
Environment="PYTHONPATH=/home/authuser/dev/chatbots/ETL_CC/src:/home/authuser/dev/chatbots/ETL_CC/.venv/lib/python3.11/site-packages"
Environment=PYTHONUNBUFFERED=1

ExecStart=/usr/bin/python3.11 -m etl_cc.langgraph_orchestrator

Restart=always
RestartSec=5
TimeoutStopSec=30

StandardOutput=journal
StandardError=journal
SyslogIdentifier=etl-cc-orchestrator

[Install]
WantedBy=multi-user.target
UNIT

  systemctl daemon-reload
}

disable_legacy_services() {
  for service in "${LEGACY_SERVICES[@]}"; do
    systemctl stop "${service}" 2>/dev/null || true
    systemctl disable "${service}" 2>/dev/null || true
  done
}

start_services() {
  install_orchestrator_unit
  disable_legacy_services

  for service in "${ACTIVE_SERVICES[@]}"; do
    systemctl enable "${service}"
    systemctl reset-failed "${service}" 2>/dev/null || true
    systemctl start "${service}"
  done

  sleep 5
}

stop_services() {
  for service in "${ACTIVE_SERVICES[@]}"; do
    systemctl stop "${service}" 2>/dev/null || true
  done
}

restart_services() {
  stop_services
  start_services
}

show_status() {
  for service in \
    "${ACTIVE_SERVICES[@]}" \
    "${LEGACY_SERVICES[@]}"
  do
    active="$(
      systemctl is-active "${service}" 2>/dev/null || true
    )"

    enabled="$(
      systemctl is-enabled "${service}" 2>/dev/null || true
    )"

    printf "%-24s active=%-12s enabled=%s\n" \
      "${service}" \
      "${active}" \
      "${enabled}"
  done
}

health_check() {
  local url="http://127.0.0.1:4046/api/v1/health"

  for attempt in {1..15}; do
    if curl --silent --fail "${url}" >/dev/null; then
      echo "ETL CC API is healthy."
      curl --silent "${url}"
      echo
      return 0
    fi

    sleep 2
  done

  echo "ETL CC API health check failed."
  return 1
}

show_logs() {
  journalctl \
    -u etl-cc-api \
    -u etl-cc-discovery \
    -u etl-cc-orchestrator \
    -f
}

case "${ACTION}" in
  start)
    require_root
    start_services
    show_status
    health_check
    ;;

  stop)
    require_root
    stop_services
    show_status
    ;;

  restart)
    require_root
    restart_services
    show_status
    health_check
    ;;

  status)
    require_root
    show_status
    ;;

  logs)
    require_root
    show_logs
    ;;

  health)
    health_check
    ;;

  *)
    echo "Usage: deploy_dev.sh {start|stop|restart|status|logs|health}"
    exit 2
    ;;
esac