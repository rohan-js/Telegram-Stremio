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
STARTUP_GRACE_SECONDS="${STARTUP_GRACE_SECONDS:-240}"

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  exit 0
fi

STAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
BASE_URL=$(grep -E "^BASE_URL=" "${CONFIG}" | cut -d= -f2- || true)
ADDON_TOKEN=$(grep -E "^DEFAULT_ADDON_TOKEN=" "${CONFIG}" | cut -d= -f2- || true)
OWNER_TG_ID=$(grep -E "^OWNER_ID=" "${CONFIG}" | cut -d= -f2- || true)
BOT_TOKEN=$(grep -E "^BOT_TOKEN=" "${CONFIG}" | cut -d= -f2- || true)

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
  docker compose version >/dev/null 2>&1 || return 1
  echo "docker compose"
}

container_state() {
  docker inspect -f '{{.State.Status}}{{if .State.Health}}/{{.State.Health.Status}}{{end}}' "${CONTAINER}" 2>/dev/null || echo "missing"
}

container_uptime_sec() {
  local started_at started_epoch now_epoch
  started_at=$(docker inspect -f '{{.State.StartedAt}}' "${CONTAINER}" 2>/dev/null || true)
  if [ -z "${started_at}" ] || [ "${started_at}" = "0001-01-01T00:00:00Z" ]; then
    echo "-1"
    return
  fi
  started_epoch=$(date -u -d "${started_at}" +%s 2>/dev/null || echo "")
  if [ -z "${started_epoch}" ]; then
    echo "-1"
    return
  fi
  now_epoch=$(date -u +%s)
  echo $((now_epoch - started_epoch))
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

# --- Owner Telegram alert (curl direct to api.telegram.org): works even
# --- when the FastAPI app is dead, unlike in-app owner_alerts. Per-event
# --- cooldown files keep a persistent problem chatty-but-not-spammy
# --- (one message per event type per 30 min). Token never echoed/logged.
ALERT_STATE_DIR="${ALERT_STATE_DIR:-/home/ubuntu/.duckdns_watchdog}"
ALERT_COOLDOWN_SEC="${ALERT_COOLDOWN_SEC:-1800}"

tg_alert() {
  local key="$1"
  local text="$2"
  [ -n "${BOT_TOKEN:-}" ] && [ -n "${OWNER_TG_ID:-}" ] || return 0
  mkdir -p "${ALERT_STATE_DIR}" 2>/dev/null || return 0
  local now last=0
  now=$(date -u +%s)
  if [ -f "${ALERT_STATE_DIR}/last_alert_${key}" ]; then
    last=$(cat "${ALERT_STATE_DIR}/last_alert_${key}" 2>/dev/null || echo 0)
  fi
  if [ $((now - last)) -lt "${ALERT_COOLDOWN_SEC}" ]; then
    return 0
  fi
  if curl -fsS --max-time 10 -o /dev/null \
      -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
      --data-urlencode "chat_id=${OWNER_TG_ID}" \
      --data-urlencode "text=${text}" 2>/dev/null; then
    echo "${now}" > "${ALERT_STATE_DIR}/last_alert_${key}"
  fi
  return 0
}

log_line() {
  local status="$1"
  shift || true
  local mem_avail_mb swap_total_mb swap_free_mb root_used_pct state uptime_sec app_response_ms active_streams last_stream_error
  mem_avail_mb=$(metric_mem_available_mb)
  swap_total_mb=$(metric_swap_total_mb)
  swap_free_mb=$(metric_swap_free_mb)
  root_used_pct=$(metric_root_used_pct)
  state=$(container_state)
  uptime_sec=$(container_uptime_sec)
  app_response_ms=$(metric_app_response_ms)
  active_streams=$(metric_active_streams)
  last_stream_error=$(metric_last_stream_error)
  echo "${STAMP} status=${status} $* mem_avail_mb=${mem_avail_mb} swap_free_mb=${swap_free_mb} swap_total_mb=${swap_total_mb} root_used_pct=${root_used_pct} container=${state} container_uptime_sec=${uptime_sec} app_response_ms=${app_response_ms} active_streams=${active_streams} last_stream_error=${last_stream_error:-none}" >> "${LOG_FILE}"
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
CONTAINER_UPTIME_SEC=$(container_uptime_sec)

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
tg_alert "degraded" "🔴 tg-stremio watchdog: DEGRADED at ${STAMP} dns=${DNS_OK} public=${PUBLIC_OK} local=${LOCAL_OK} container=${CONTAINER_OK} mem_crit=${MEM_CRITICAL} disk_crit=${DISK_CRITICAL} streams=${ACTIVE_STREAMS}"

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
    tg_alert "restart_skipped" "⏳ tg-stremio watchdog: restart SKIPPED (${RESTART_REASON}) — ${ACTIVE_STREAMS} active stream(s) at ${STAMP}"
  elif [ "${RESTART_REASON}" != "memory_critical" ] && [ "${CONTAINER_UPTIME_SEC}" -ge 0 ] 2>/dev/null && [ "${CONTAINER_UPTIME_SEC}" -lt "${STARTUP_GRACE_SECONDS}" ] 2>/dev/null; then
    log_line "restart_skipped" "reason=${RESTART_REASON} startup_grace=true uptime_sec=${CONTAINER_UPTIME_SEC} grace_sec=${STARTUP_GRACE_SECONDS}"
  elif docker inspect "${CONTAINER}" >/dev/null 2>&1; then
    log_line "restarting" "reason=${RESTART_REASON}"
    tg_alert "restarting" "🔁 tg-stremio watchdog: restarting ${CONTAINER} (${RESTART_REASON}) at ${STAMP}"
    docker restart "${CONTAINER}" >/dev/null || true
  else
    if COMPOSE_CMD=$(compose_cmd); then
      log_line "recreating" "reason=${RESTART_REASON} compose=${COMPOSE_CMD// /_}"
      tg_alert "restarting" "🔁 tg-stremio watchdog: recreating ${CONTAINER} (${RESTART_REASON}) at ${STAMP}"
      (cd "${APP_DIR}" && ${COMPOSE_CMD} up -d --no-build --remove-orphans) || true
    else
      log_line "recreate_failed" "reason=${RESTART_REASON} docker_compose_v2=missing"
      tg_alert "recreate_failed" "🔴 tg-stremio watchdog: RECREATE FAILED (${RESTART_REASON}, docker compose v2 missing) at ${STAMP} — manual intervention needed"
    fi
  fi
fi

wait_for_local_ready 120 || true

if curl -fsS --max-time 15 -o /dev/null "${MANIFEST_URL}" && check_local_ready; then
  log_line "recovered"
  tg_alert "recovered" "✅ tg-stremio watchdog: RECOVERED at ${STAMP}"
  exit 0
fi

log_line "still_degraded"
tg_alert "still_degraded" "🔴 tg-stremio watchdog: STILL DEGRADED at ${STAMP} after recovery attempt (mem=${MEM_AVAIL_MB}MB root_used=${ROOT_USED_PCT}%)"
exit 1
