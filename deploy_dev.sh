#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-status}"

SERVICES=(
  etl-cc-api
  etl-cc-discovery
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

start_services() {
  systemctl daemon-reload

  for service in "${SERVICES[@]}"; do
    systemctl reset-failed "${service}" 2>/dev/null || true
    systemctl start "${service}"
  done

  sleep 5
}

stop_services() {
  for service in "${SERVICES[@]}"; do
    systemctl stop "${service}" 2>/dev/null || true
  done
}

restart_services() {
  systemctl daemon-reload

  for service in "${SERVICES[@]}"; do
    systemctl reset-failed "${service}" 2>/dev/null || true
    systemctl restart "${service}"
  done

  sleep 5
}

show_status() {
  for service in "${SERVICES[@]}"; do
    active="$(systemctl is-active "${service}" 2>/dev/null || true)"
    enabled="$(systemctl is-enabled "${service}" 2>/dev/null || true)"

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
      echo "Swagger: http://74.225.207.59:4046/docs"
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
    -u etl-cc-conversion \
    -u etl-cc-validation \
    -u etl-cc-deployment \
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
    echo "Usage:"
    echo "  sudo ./deploy_dev.sh start"
    echo "  sudo ./deploy_dev.sh stop"
    echo "  sudo ./deploy_dev.sh restart"
    echo "  sudo ./deploy_dev.sh status"
    echo "  sudo ./deploy_dev.sh logs"
    echo "  ./deploy_dev.sh health"
    exit 1
    ;;
esac