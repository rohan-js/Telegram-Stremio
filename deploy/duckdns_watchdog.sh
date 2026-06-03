#!/usr/bin/env bash
set -euo pipefail

CONFIG="${CONFIG:-/home/ubuntu/telegram-stremio/config.env}"
LOG_FILE="${LOG_FILE:-/home/ubuntu/duckdns-watchdog.log}"
LOCK_FILE="${LOCK_FILE:-/tmp/duckdns_watchdog.lock}"
APP_DIR="${APP_DIR:-/home/ubuntu/telegram-stremio}"
CONTAINER="${CONTAINER:-tg_stremio}"
LOCAL_URL="${LOCAL_URL:-http://127.0.0.1:8000/login}"
DUCKDNS_UPDATE="${DUCKDNS_UPDATE:-/home/ubuntu/duckdns_update.sh}"
STREAM_STATS_URL="${STREAM_STATS_URL:-http://127.0.0.1:8000/stream/stats}"

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

compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    echo "docker compose"
  else
    echo "docker-compose"
  fi
}

container_state() {
  docker inspect -f '{{.State.Status}}{{if .State.Health}}/{{.State.Health.Status}}{{end}}' "${CONTAINER}" 2>/dev/null || echo "missing"
}

check_local_ready() {
  curl -fsS --max-time 5 -o /dev/null "${LOCAL_URL}"
}

metric_app_response_ms() {
  local value
  value=$(curl -fsS --max-time 8 -o /dev/null -w '%{time_total}' "${LOCAL_URL}" 2>/dev/null || echo "")
  if [ -z "${value}" ]; then
    echo "-1"
  else
    awk -v sec="${value}" 'BEGIN { printf "%d", sec * 1000 }'
  fi
}

metric_active_streams() {
  curl -fsS --max-time 5 "${STREAM_STATS_URL}" 2>/dev/null \
    | python3 -c 'import sys,json; print(len(json.load(sys.stdin).get("active_streams", [])))' 2>/dev/null \
    || echo "-1"
}

metric_last_stream_error() {
  docker logs --tail 200 "${CONTAINER}" 2>/dev/null \
    | grep -Ei 'stream chunk failure|chunk timeout|consumer_error|producer_error|Traceback|ERROR' \
    | tail -1 \
    | tr ' ' '_' \
    | cut -c1-240 \
    || true
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
  local mem_avail_mb swap_total_mb swap_free_mb root_used_pct state app_response_ms active_streams last_stream_error
  mem_avail_mb=$(metric_mem_available_mb)
  swap_total_mb=$(metric_swap_total_mb)
  swap_free_mb=$(metric_swap_free_mb)
  root_used_pct=$(metric_root_used_pct)
  state=$(container_state)
  app_response_ms=$(metric_app_response_ms)
  active_streams=$(metric_active_streams)
  last_stream_error=$(metric_last_stream_error)
  echo "${STAMP} status=${status} $* mem_avail_mb=${mem_avail_mb} swap_free_mb=${swap_free_mb} swap_total_mb=${swap_total_mb} root_used_pct=${root_used_pct} container=${state} app_response_ms=${app_response_ms} active_streams=${active_streams} last_stream_error=${last_stream_error:-none}" >> "${LOG_FILE}"
}

DNS_OK=0
PUBLIC_OK=0
LOCAL_OK=0
CONTAINER_OK=0
MEM_CRITICAL=0
DISK_CRITICAL=0
ACTIVE_STREAMS=0

getent hosts "${HOST}" >/dev/null 2>&1 && DNS_OK=1
curl -fsS --max-time 15 -o /dev/null "${MANIFEST_URL}" && PUBLIC_OK=1 || true
check_local_ready && LOCAL_OK=1 || true
ACTIVE_STREAMS=$(metric_active_streams)

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

RESTART_REASON=""
if [ "${LOCAL_OK}" -eq 0 ]; then
  RESTART_REASON="local_unhealthy"
elif [ "${CONTAINER_OK}" -eq 0 ]; then
  RESTART_REASON="container_unhealthy"
elif [ "${MEM_CRITICAL}" -eq 1 ]; then
  RESTART_REASON="memory_critical"
fi

if [ -n "${RESTART_REASON}" ]; then
  if [ "${ACTIVE_STREAMS}" -gt 0 ] 2>/dev/null && [ "${LOCAL_OK}" -eq 1 ]; then
    log_line "restart_skipped" "reason=${RESTART_REASON} active_streams=${ACTIVE_STREAMS}"
  elif docker inspect "${CONTAINER}" >/dev/null 2>&1; then
    log_line "restarting" "reason=${RESTART_REASON}"
    docker restart "${CONTAINER}" >/dev/null || true
  else
    COMPOSE_CMD=$(compose_cmd)
    log_line "recreating" "reason=${RESTART_REASON} compose=${COMPOSE_CMD// /_}"
    (cd "${APP_DIR}" && ${COMPOSE_CMD} up -d --no-build --remove-orphans) || true
  fi
fi

wait_for_local_ready 120 || true

if curl -fsS --max-time 15 -o /dev/null "${MANIFEST_URL}" && check_local_ready; then
  log_line "recovered"
  exit 0
fi

log_line "still_degraded"
exit 1
