#!/usr/bin/env bash
set -euo pipefail

CONFIG="${CONFIG:-/home/ubuntu/telegram-stremio/config.env}"
LOG_FILE="${LOG_FILE:-/home/ubuntu/duckdns-watchdog.log}"
LOCK_FILE="${LOCK_FILE:-/tmp/duckdns_watchdog.lock}"
APP_DIR="${APP_DIR:-/home/ubuntu/telegram-stremio}"
CONTAINER="${CONTAINER:-tg_stremio}"
LOCAL_URL="${LOCAL_URL:-http://127.0.0.1:8000/login}"
DUCKDNS_UPDATE="${DUCKDNS_UPDATE:-/home/ubuntu/duckdns_update.sh}"

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  exit 0
fi

STAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
BASE_URL=$(grep -E "^BASE_URL=" "${CONFIG}" | cut -d= -f2- || true)
ADDON_TOKEN=$(grep -E "^DEFAULT_ADDON_TOKEN=" "${CONFIG}" | cut -d= -f2- || true)

if [ -z "${BASE_URL}" ] || [ -z "${ADDON_TOKEN}" ]; then
  echo "${STAMP} status=error reason=missing_config_values" >> "${LOG_FILE}"
  exit 1
fi

HOST=$(echo "${BASE_URL}" | sed -E "s#https?://([^/]+).*#\1#")
MANIFEST_URL="${BASE_URL%/}/stremio/${ADDON_TOKEN}/manifest.json"

metric_mem_available_mb() {
  awk '/MemAvailable:/ { print int($2 / 1024) }' /proc/meminfo
}

metric_swap_total_mb() {
  awk '/SwapTotal:/ { print int($2 / 1024) }' /proc/meminfo
}

metric_swap_free_mb() {
  awk '/SwapFree:/ { print int($2 / 1024) }' /proc/meminfo
}

metric_root_used_pct() {
  df -P / | awk 'NR == 2 { gsub("%", "", $5); print $5 }'
}

container_state() {
  docker inspect -f '{{.State.Status}}{{if .State.Health}}/{{.State.Health.Status}}{{end}}' "${CONTAINER}" 2>/dev/null || echo "missing"
}

check_local_ready() {
  curl -fsS --max-time 5 -o /dev/null "${LOCAL_URL}"
}

wait_for_local_ready() {
  local timeout_sec=${1:-120}
  local waited=0
  while [ "${waited}" -lt "${timeout_sec}" ]; do
    if check_local_ready; then
      return 0
    fi
    sleep 5
    waited=$((waited + 5))
  done
  return 1
}

log_line() {
  local status="$1"
  shift || true
  local mem_avail_mb swap_total_mb swap_free_mb root_used_pct state
  mem_avail_mb=$(metric_mem_available_mb)
  swap_total_mb=$(metric_swap_total_mb)
  swap_free_mb=$(metric_swap_free_mb)
  root_used_pct=$(metric_root_used_pct)
  state=$(container_state)
  echo "${STAMP} status=${status} $* mem_avail_mb=${mem_avail_mb} swap_free_mb=${swap_free_mb} swap_total_mb=${swap_total_mb} root_used_pct=${root_used_pct} container=${state}" >> "${LOG_FILE}"
}

DNS_OK=0
PUBLIC_OK=0
LOCAL_OK=0
CONTAINER_OK=0
MEM_CRITICAL=0
DISK_CRITICAL=0

getent hosts "${HOST}" >/dev/null 2>&1 && DNS_OK=1
curl -fsS --max-time 15 -o /dev/null "${MANIFEST_URL}" && PUBLIC_OK=1 || true
check_local_ready && LOCAL_OK=1 || true

STATE=$(container_state)
case "${STATE}" in
  running|running/healthy|running/starting) CONTAINER_OK=1 ;;
esac

MEM_AVAIL_MB=$(metric_mem_available_mb)
SWAP_TOTAL_MB=$(metric_swap_total_mb)
SWAP_FREE_MB=$(metric_swap_free_mb)
ROOT_USED_PCT=$(metric_root_used_pct)

if [ "${MEM_AVAIL_MB}" -lt 80 ] && { [ "${SWAP_TOTAL_MB}" -eq 0 ] || [ "${SWAP_FREE_MB}" -lt 128 ]; }; then
  MEM_CRITICAL=1
fi

if [ "${ROOT_USED_PCT}" -ge 95 ]; then
  DISK_CRITICAL=1
fi

if [ "${DNS_OK}" -eq 1 ] && [ "${PUBLIC_OK}" -eq 1 ] && [ "${LOCAL_OK}" -eq 1 ] && [ "${CONTAINER_OK}" -eq 1 ] && [ "${MEM_CRITICAL}" -eq 0 ] && [ "${DISK_CRITICAL}" -eq 0 ]; then
  log_line "ok"
  exit 0
fi

log_line "degraded" "dns_ok=${DNS_OK} public_ok=${PUBLIC_OK} local_ok=${LOCAL_OK} container_ok=${CONTAINER_OK} mem_critical=${MEM_CRITICAL} disk_critical=${DISK_CRITICAL}"

if [ -x "${DUCKDNS_UPDATE}" ]; then
  "${DUCKDNS_UPDATE}" || true
fi

sudo nginx -t && sudo systemctl reload nginx || true

if [ "${LOCAL_OK}" -eq 0 ] || [ "${CONTAINER_OK}" -eq 0 ] || [ "${MEM_CRITICAL}" -eq 1 ]; then
  if docker inspect "${CONTAINER}" >/dev/null 2>&1; then
    docker restart "${CONTAINER}" >/dev/null || true
  else
    (cd "${APP_DIR}" && docker-compose up -d --no-build --remove-orphans) || true
  fi
fi

wait_for_local_ready 120 || true

if curl -fsS --max-time 15 -o /dev/null "${MANIFEST_URL}" && check_local_ready; then
  log_line "recovered"
  exit 0
fi

log_line "still_degraded"
exit 1
