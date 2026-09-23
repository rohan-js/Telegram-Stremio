#!/usr/bin/env bash
# ============================================================================
# External dead-man's switch for the Telegram-Stremio VPS.
#
# Runs on the FILELINK VPS (68.233.108.188) via ubuntu crontab every 5 min,
# probes the STREMIO VPS publicly, alerts the owner on Telegram, and — once
# armed — auto-reboots the instance after a sustained outage (owner opted in
# 2026-09-23 after the Sep-15 and Sep-23 freezes required manual Console
# force-stop/start).
#
# State machine (per 5-min cycle):
#   fail 1 cycle                -> still UP (single blip absorbed)
#   2 consecutive failed cycles -> DOWN + Telegram alert
#   every 12 cycles while DOWN  -> re-alert (1 h cadence, persistent not spammy)
#   3rd failed cycle (~15 min)  -> auto-reboot via `oci ... --action RESET`
#                                   (max 1 per 30 min, once per incident)
#   still DOWN 10 min post-reboot -> "needs manual Console intervention"
#   any success after DOWN      -> RECOVERED alert (+ reboot mention), reset
#
# A cycle fails only when BOTH probes fail (/login and /), so one healthy
# endpoint absorbs transient single-endpoint flaps (no false reboots).
#
# Modes (first match wins):
#   WATCHDOG_DRY_RUN=1 env      -> print every action, send nothing
#   ${STATE_DIR}/mode = dry-run -> same, installer-controlled (preferred;
#                                   flip to "armed" to enable for real)
#
# Secrets: bot token lives in ${STATE_DIR}/tg (0600, ubuntu-only, never
# printed/logged); OCI private key in ${HOME}/.oci/ (0600) — created by the
# owner interactively. Never echo either.
# ============================================================================
set -uo pipefail

STATE_DIR="${STATE_DIR:-/home/ubuntu/.stremio_watchdog}"
STATE_FILE="${STATE_DIR}/state"
TG_FILE="${STATE_DIR}/tg"                 # line 1: bot token, line 2: owner chat id
CFG_FILE="${STATE_DIR}/config"            # INSTANCE_OCID=..., INSTANCE_NAME=...
WATCHDOG_LOG="${STATE_DIR}/watchdog.log"

PROBE_BASE="${WATCHDOG_PROBE_BASE:-https://telegram-stremio.duckdns.org}"
PROBE_TIMEOUT="${PROBE_TIMEOUT:-10}"
FAIL_THRESHOLD="${FAIL_THRESHOLD:-2}"       # consecutive fails -> DOWN
REBOOT_FAIL_COUNT="${REBOOT_FAIL_COUNT:-3}" # failed cycles before RESET (~15 min)
RE_ALERT_CYCLES="${RE_ALERT_CYCLES:-12}"    # re-alert cadence while DOWN
REBOOT_COOLDOWN_SEC="${REBOOT_COOLDOWN_SEC:-1800}"   # max 1 RESET / 30 min
POST_REBOOT_VERIFY_SEC="${POST_REBOOT_VERIFY_SEC:-600}"  # 10 min -> manual
DEFAULT_OWNER_ID="5422223708"

# ---------------------------------------------------------------------------
# Mode resolution: env DRY_RUN wins, then mode file, default armed.
# ---------------------------------------------------------------------------
mode_now() {
  if [ "${WATCHDOG_DRY_RUN:-0}" = "1" ]; then
    echo "dry-run"
    return
  fi
  local m
  m=$(cat "${STATE_DIR}/mode" 2>/dev/null || echo "armed")
  case "${m}" in
    dry-run|armed) echo "${m}" ;;
    *) echo "armed" ;;
  esac
}
MODE=$(mode_now)

logw() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "${WATCHDOG_LOG}" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Install helper: pull the file_stream_bot token out of the running
# container's env into ${TG_FILE} (0600). Never prints the token.
# ---------------------------------------------------------------------------
grab_token() {
  mkdir -p "${STATE_DIR}"
  chmod 700 "${STATE_DIR}"
  local token="" token_file="" owner=""
  token=$(docker inspect file_stream_bot --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null \
    | sed -n 's/^BOT_TOKEN=//p' | head -1)
  if [ -z "${token}" ]; then
    # fallback: compose env files (file_stream_bot uses env_file, so the
    # token never appears in docker inspect Config.Env)
    local f
    for f in /home/ubuntu/file-stream-bot/config.env \
             /home/ubuntu/filelink/*.env /home/ubuntu/filelink/.env; do
      [ -f "${f}" ] || continue
      token=$(sed -n 's/^BOT_TOKEN=//p' "${f}" 2>/dev/null | head -1)
      if [ -n "${token}" ]; then
        token_file="${f}"
        break
      fi
    done
  fi
  if [ -z "${token}" ]; then
    echo "ERROR: could not locate BOT_TOKEN (container env or compose env file)" >&2
    return 1
  fi
  if [ -n "${token_file}" ]; then
    owner=$(sed -n 's/^OWNER_ID=//p' "${token_file}" 2>/dev/null | head -1)
  fi
  [ -n "${owner}" ] || owner="${DEFAULT_OWNER_ID}"
  {
    printf '%s\n' "${token}"
    printf '%s\n' "${owner}"
  } > "${TG_FILE}"
  chmod 600 "${TG_FILE}"
  echo "token stored in ${TG_FILE} (mode 600)."
  return 0
}

# ---------------------------------------------------------------------------
# Telegram send (dry-run prints instead). Token never appears in output.
# ---------------------------------------------------------------------------
TG_TOKEN=""
TG_OWNER=""
load_tg() {
  [ -r "${TG_FILE}" ] || return 1
  TG_TOKEN=$(sed -n '1p' "${TG_FILE}")
  TG_OWNER=$(sed -n '2p' "${TG_FILE}")
  [ -n "${TG_TOKEN}" ] && [ -n "${TG_OWNER}" ]
}

tg_send() {
  local text="$1"
  if [ "${MODE}" = "dry-run" ]; then
    echo "[DRY_RUN] TG->owner: ${text}"
    logw "dry_run tg: ${text}"
    return 0
  fi
  if ! load_tg; then
    logw "warn: tg token missing (${TG_FILE}) — alert not sent"
    return 0
  fi
  curl -fsS --max-time 10 -o /dev/null \
    -X POST "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
    --data-urlencode "chat_id=${TG_OWNER}" \
    --data-urlencode "text=${text}" 2>/dev/null \
    && logw "tg_sent: ${text:0:120}" \
    || logw "warn: tg send failed"
  return 0
}

# ---------------------------------------------------------------------------
# Config (OCID etc.)
# ---------------------------------------------------------------------------
INSTANCE_OCID=""
INSTANCE_NAME="telegram-stremio-ubuntu (152.67.163.120)"
load_cfg() {
  [ -r "${CFG_FILE}" ] || return 1
  INSTANCE_OCID=$(sed -n 's/^INSTANCE_OCID=//p' "${CFG_FILE}" | head -1)
  local n
  n=$(sed -n 's/^INSTANCE_NAME=//p' "${CFG_FILE}" | head -1)
  [ -n "${n}" ] && INSTANCE_NAME="${n}"
  [ -n "${INSTANCE_OCID}" ]
}

# ---------------------------------------------------------------------------
# Auto-reboot via oci-cli (runs as ubuntu; config+key in ~/.oci, 0600).
# ---------------------------------------------------------------------------
find_oci() {
  # cron doesn't source ~/.profile, so check install locations explicitly.
  if [ -x "${HOME}/bin/oci" ]; then
    echo "${HOME}/bin/oci"
  elif [ -x "${HOME}/.local/bin/oci" ]; then
    echo "${HOME}/.local/bin/oci"
  elif command -v oci >/dev/null 2>&1; then
    command -v oci
  else
    return 1
  fi
}

do_reboot() {
  # Prints the failure reason on stdout (caller alert-once's it) — never
  # sends Telegram directly here, so a missing key can't DM-spam every cycle.
  if [ "${MODE}" = "dry-run" ]; then
    echo "[DRY_RUN] would run: oci compute instance action --instance-id <stremio-ocid> --action RESET"
    logw "dry_run reboot (not executed)"
    return 0
  fi
  if ! load_cfg; then
    echo "config missing INSTANCE_OCID in ${CFG_FILE}"
    logw "error: no OCID configured, reboot skipped"
    return 1
  fi
  local oci_bin
  if ! oci_bin=$(find_oci); then
    echo "oci-cli not installed"
    logw "error: oci-cli missing"
    return 1
  fi
  logw "action: RESET ${INSTANCE_OCID}"
  if "${oci_bin}" compute instance action \
      --instance-id "${INSTANCE_OCID}" \
      --action RESET \
      --auth config_file 2>>"${WATCHDOG_LOG}"; then
    echo "[auto-reboot issued]"
    return 0
  fi
  echo "oci-cli error (see ${WATCHDOG_LOG})"
  logw "error: oci RESET failed"
  return 1
}

# ---------------------------------------------------------------------------
# Subcommand dispatch (install-time helpers).
# ---------------------------------------------------------------------------
case "${1:-}" in
  --grab-token)
    grab_token
    exit $?
    ;;
  --print-mode)
    echo "${MODE}"
    exit 0
    ;;
  ""|--cycle)
    ;;   # fall through to a normal watchdog cycle
  *)
    echo "usage: $0 [--grab-token|--print-mode|--cycle]" >&2
    exit 2
    ;;
esac

# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------
sget() { sed -n "s/^$1=//p" "${STATE_FILE}" 2>/dev/null | head -1; }

save_state() {
  # args as key=value pairs
  local tmp="${STATE_FILE}.tmp"
  : > "${tmp}"
  local kv
  for kv in "$@"; do
    printf '%s\n' "${kv}" >> "${tmp}"
  done
  mv -f "${tmp}" "${STATE_FILE}"
  chmod 600 "${STATE_FILE}" 2>/dev/null || true
}

now_epoch() { date -u +%s; }

# ---------------------------------------------------------------------------
# Probe: cycle fails only when BOTH endpoints fail.
# ---------------------------------------------------------------------------
probe_cycle() {
  local login_ok=0 root_ok=0
  if curl -fsS --max-time "${PROBE_TIMEOUT}" -o /dev/null "${PROBE_BASE}/login" 2>/dev/null; then
    login_ok=1
  fi
  if curl -fsS --max-time "${PROBE_TIMEOUT}" -o /dev/null "${PROBE_BASE}/" 2>/dev/null; then
    root_ok=1
  fi
  PROBE_LOGIN="${login_ok}"
  PROBE_ROOT="${root_ok}"
  [ "${login_ok}" -eq 1 ] || [ "${root_ok}" -eq 1 ]
}

# ---------------------------------------------------------------------------
# One watchdog cycle
# ---------------------------------------------------------------------------
mkdir -p "${STATE_DIR}" 2>/dev/null || true

STAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if probe_cycle; then
  CYCLE_OK=1
else
  CYCLE_OK=0
fi

STATUS=$(sget status);        [ -n "${STATUS}" ] || STATUS="UP"
FAIL_COUNT=$(sget fail_count);[ -n "${FAIL_COUNT}" ] || FAIL_COUNT=0
CYCLES_SINCE_ALERT=$(sget cycles_since_alert); [ -n "${CYCLES_SINCE_ALERT}" ] || CYCLES_SINCE_ALERT=0
REBOOT_FIRED=$(sget reboot_fired); [ -n "${REBOOT_FIRED}" ] || REBOOT_FIRED=0
LAST_REBOOT=$(sget last_reboot_epoch); [ -n "${LAST_REBOOT}" ] || LAST_REBOOT=0
MANUAL_ALERTED=$(sget manual_alerted); [ -n "${MANUAL_ALERTED}" ] || MANUAL_ALERTED=0
SKIP_ALERTED=$(sget skip_alerted); [ -n "${SKIP_ALERTED}" ] || SKIP_ALERTED=0
INCIDENT=$(sget incident);    [ -n "${INCIDENT}" ] || INCIDENT=0

if [ "${CYCLE_OK}" -eq 1 ]; then
  if [ "${STATUS}" = "DOWN" ]; then
    REBOOT_NOTE=""
    [ "${REBOOT_FIRED}" -eq 1 ] && REBOOT_NOTE=" (auto-reboot had been issued)"
    tg_send "✅ stremio-watchdog: RECOVERED after ${FAIL_COUNT} failed cycle(s)${REBOOT_NOTE} — ${INSTANCE_NAME} UP at ${STAMP}"
    logw "status=RECOVERED fail_count=${FAIL_COUNT} reboot_fired=${REBOOT_FIRED} login=${PROBE_LOGIN} root=${PROBE_ROOT}"
    INCIDENT=$((INCIDENT + 1))
    save_state "status=UP" "fail_count=0" "cycles_since_alert=0" \
               "reboot_fired=0" "last_reboot_epoch=${LAST_REBOOT}" \
               "manual_alerted=0" "skip_alerted=0" "incident=${INCIDENT}" "last_ok_epoch=$(now_epoch)"
  else
    logw "status=ok login=${PROBE_LOGIN} root=${PROBE_ROOT}"
    save_state "status=UP" "fail_count=0" "cycles_since_alert=0" \
               "reboot_fired=0" "last_reboot_epoch=${LAST_REBOOT}" \
               "manual_alerted=0" "skip_alerted=0" "incident=${INCIDENT}" "last_ok_epoch=$(now_epoch)"
  fi
  exit 0
fi

# ---- failed cycle ----
FAIL_COUNT=$((FAIL_COUNT + 1))
CYCLES_SINCE_ALERT=$((CYCLES_SINCE_ALERT + 1))
NOW=$(now_epoch)
ACTION="none"

if [ "${FAIL_COUNT}" -ge "${FAIL_THRESHOLD}" ]; then
  if [ "${STATUS}" != "DOWN" ]; then
    STATUS="DOWN"
    CYCLES_SINCE_ALERT=0
    tg_send "🔴 stremio-watchdog: ${INSTANCE_NAME} DOWN (probe failed ${FAIL_COUNT}x: login=${PROBE_LOGIN} root=${PROBE_ROOT}) at ${STAMP}"
    ACTION="alert_down"
  elif [ "${CYCLES_SINCE_ALERT}" -ge "${RE_ALERT_CYCLES}" ]; then
    CYCLES_SINCE_ALERT=0
    tg_send "🔴 stremio-watchdog: ${INSTANCE_NAME} STILL DOWN (cycle ${FAIL_COUNT}, reboot_fired=${REBOOT_FIRED}) at ${STAMP}"
    ACTION="realert"
  fi
fi

# Auto-reboot: 3rd consecutive failed cycle, once per incident, 30-min cooldown.
if [ "${FAIL_COUNT}" -ge "${REBOOT_FAIL_COUNT}" ] && [ "${REBOOT_FIRED}" -eq 0 ]; then
  if [ $((NOW - LAST_REBOOT)) -ge "${REBOOT_COOLDOWN_SEC}" ]; then
    if REBOOT_OUT=$(do_reboot); then
      REBOOT_FIRED=1
      LAST_REBOOT="${NOW}"
      ACTION="reboot"
      echo "${REBOOT_OUT}"     # dry-run action line / "[auto-reboot issued]"
      tg_send "🔁 stremio-watchdog: auto-reboot (RESET) issued for ${INSTANCE_NAME} at ${STAMP} — expecting recovery in ~2 min."
    else
      # Reboot not possible (missing key/OCID/oci-cli): alert ONCE per
      # incident, then only log — a long outage must not DM every 5 min.
      if [ "${SKIP_ALERTED}" -eq 0 ]; then
        tg_send "🚨 stremio-watchdog: ${INSTANCE_NAME} DOWN and auto-reboot NOT possible (${REBOOT_OUT}) — needs manual Console intervention."
        SKIP_ALERTED=1
        ACTION="manual_needed"
      fi
    fi
  else
    logw "reboot skipped: cooldown ($((NOW - LAST_REBOOT))s < ${REBOOT_COOLDOWN_SEC}s)"
  fi
fi

# Post-reboot verification: still DOWN 10 min after the reboot -> manual.
if [ "${REBOOT_FIRED}" -eq 1 ] && [ "${MANUAL_ALERTED}" -eq 0 ] \
   && [ $((NOW - LAST_REBOOT)) -ge "${POST_REBOOT_VERIFY_SEC}" ]; then
  tg_send "🚨 stremio-watchdog: ${INSTANCE_NAME} STILL DOWN 10 min after auto-reboot — needs manual Console intervention (force-stop → start)."
  MANUAL_ALERTED=1
  ACTION="manual_needed"
fi

save_state "status=${STATUS}" "fail_count=${FAIL_COUNT}" \
           "cycles_since_alert=${CYCLES_SINCE_ALERT}" \
           "reboot_fired=${REBOOT_FIRED}" "last_reboot_epoch=${LAST_REBOOT}" \
           "manual_alerted=${MANUAL_ALERTED}" "skip_alerted=${SKIP_ALERTED}" \
           "incident=${INCIDENT}"

logw "status=fail fail_count=${FAIL_COUNT} down=$( [ "${STATUS}" = "DOWN" ] && echo 1 || echo 0 ) login=${PROBE_LOGIN} root=${PROBE_ROOT} action=${ACTION} mode=${MODE}"
exit 1
