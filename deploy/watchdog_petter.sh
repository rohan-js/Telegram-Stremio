#!/usr/bin/env bash
# On-box last-resort freeze recovery: pets the softdog kernel watchdog only
# while the box is healthy. Stops petting when the app is unresponsive AND
# the box is extremely loaded/memory-starved for 3+ minutes -> softdog
# hard-reboots the VM. This is the same recovery the owner performs from
# the Oracle Console, but automatic and in ~1-5 minutes instead of
# "whenever someone notices". The external Filelink watchdog (15 min ->
# oci RESET) remains the backstop for the case where even this fails.
#
# Fail-safe by design:
#   - petter crashes (fd closed by kernel): watchdog disarms (softdog
#     nowayout=0), systemd restarts the petter 5 s later -> re-armed,
#     no false reboot
#   - SIGTERM/SIGINT (systemctl stop): writes the magic 'V' byte first
#     -> clean disarm, safe maintenance path
#   - total userland starvation: the petter freezes WITH the fd open and
#     stops writing -> watchdog fires -> reboot (exactly the intent)
# The module is loaded by the unit's ExecStartPre, not modules-load.d:
# the 60 s countdown starts when this script opens the device, so a slow
# boot can never outrun the watchdog into a boot loop.
#
# 2026-10-18 additions: boot-id change detection (alerts the owner on ANY
# host reboot, including a silent petter-triggered softdog reboot), a
# per-loop heartbeat file for post-mortem forensics, and a best-effort
# pre-withhold "starving" DM. The petter starts at every boot and runs as
# root, so this is the only notification path that survives the app, Docker
# and WARP being down.
set -u
PET_INTERVAL=5
FAIL_LIMIT=36          # consecutive failed logins = 3 min
LOAD_LIMIT=6.0         # 1-min load that counts as extreme (box has ~2 threads)
MEM_LIMIT_MB=60        # MemAvailable (MB) that counts as extreme
LOGIN_URL="http://127.0.0.1:8000/login"
LOG_DIR=/home/ubuntu/.watchdog_petter
LOG="$LOG_DIR/petter.log"
CONFIG="${CONFIG:-/home/ubuntu/telegram-stremio/config.env}"
BOOT_ID_FILE="$LOG_DIR/boot_id"
HEARTBEAT_FILE="$LOG_DIR/heartbeat"

mkdir -p "$LOG_DIR" && chmod 700 "$LOG_DIR"
log() { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') $*" >> "$LOG"; }
trim_log() {
  size=$(stat -c %s "$LOG" 2>/dev/null || echo 0)
  if [ "$size" -gt 200000 ]; then tail -c 50000 "$LOG" > "$LOG.t" && mv "$LOG.t" "$LOG"; fi
}

# --- Owner Telegram alert (direct to api.telegram.org): works with the app
# --- dead, unlike in-app owner_alerts. Token never echoed/logged.
BOT_TOKEN=$(grep -E "^BOT_TOKEN=" "$CONFIG" 2>/dev/null | cut -d= -f2- || true)
OWNER_TG_ID=$(grep -E "^OWNER_ID=" "$CONFIG" 2>/dev/null | cut -d= -f2- || true)
tg_alert() {
  local text="$1"
  [ -n "${BOT_TOKEN:-}" ] && [ -n "${OWNER_TG_ID:-}" ] || return 0
  curl -fsS --max-time 8 -o /dev/null \
    -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
    --data-urlencode "chat_id=${OWNER_TG_ID}" \
    --data-urlencode "text=${text}" 2>/dev/null || true
}

[ -e /dev/watchdog ] || { echo "softdog device missing" >&2; exit 1; }
exec 3>/dev/watchdog || { echo "cannot open /dev/watchdog" >&2; exit 1; }

disarm() { printf 'V' >&3 2>/dev/null || true; }
trap 'disarm; exit 0' TERM INT

log "petter started (interval=${PET_INTERVAL}s fail_limit=${FAIL_LIMIT} load_limit=${LOAD_LIMIT} mem_limit=${MEM_LIMIT_MB}MB)"

# --- Boot-id change detection. First run against this state dir seeds the
# --- id silently; any later change means the VM rebooted (including a
# --- petter-triggered softdog reboot, which used to be completely silent).
BOOT_ID=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo unknown)
PREV_BOOT_ID=$(cat "$BOOT_ID_FILE" 2>/dev/null || echo "")
echo "$BOOT_ID" > "$BOOT_ID_FILE" 2>/dev/null || true
if [ -z "$PREV_BOOT_ID" ]; then
  log "boot_id seeded (${BOOT_ID:0:8}) - first petter run against this state dir"
elif [ "$PREV_BOOT_ID" != "$BOOT_ID" ]; then
  log "HOST REBOOT detected (boot_id ${PREV_BOOT_ID:0:8} -> ${BOOT_ID:0:8})"
  tg_alert "🔁 Host reboot detected on the Stremio VPS (boot-id ${PREV_BOOT_ID:0:8}→${BOOT_ID:0:8}). If unexpected (freeze / softdog auto-reboot), check /home/ubuntu/.watchdog_petter/petter.log and the duckdns-watchdog log."
fi

fails=0
while :; do
  printf '%s' "$(date -u +%s)" > "$HEARTBEAT_FILE" 2>/dev/null || true
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$LOGIN_URL" 2>/dev/null || echo 000)
  if [ "$code" = "200" ]; then
    if [ "$fails" -ge "$FAIL_LIMIT" ]; then
      log "recovered after $fails failed checks (no reboot)"
    fi
    if [ -f "$LOG_DIR/starved_alerted" ]; then rm -f "$LOG_DIR/starved_alerted"; fi
    fails=0
    printf 'R' >&3 2>/dev/null || true
  else
    fails=$((fails + 1))
    load1=$(awk '{print $1}' /proc/loadavg 2>/dev/null || echo 0)
    avail=$(awk '/MemAvailable/{print int($2/1024); exit}' /proc/meminfo 2>/dev/null || echo 99999)
    extreme=$(awk -v l="$load1" -v m="$avail" -v L="$LOAD_LIMIT" -v M="$MEM_LIMIT_MB" 'BEGIN{print (l>L || m<M) ? 1 : 0}')
    if [ "$extreme" = "1" ] && [ "$fails" -ge "$FAIL_LIMIT" ]; then
      log "STARVED: login down ${fails} checks, load=$load1, avail=${avail}MB - withholding pets, softdog reboots in <=60s"
      # Best-effort pre-withhold DM; it can lose the race against a hard
      # freeze, which is why the boot-id DM above is the guarantee.
      if [ ! -f "$LOG_DIR/starved_alerted" ]; then
        tg_alert "⚠️ Stremio VPS starving: login down ${fails} checks, load=${load1}, avail=${avail}MB. Petter withholding - softdog will hard-reboot within ~60s if the box does not recover."
        touch "$LOG_DIR/starved_alerted" 2>/dev/null || true
      fi
      sleep "$PET_INTERVAL"
      continue
    fi
    # App down but the box is not (yet) starving: keep petting. The
    # duckdns watchdog owns app-level recovery; rebooting the VM for a
    # mere app crash would be overkill.
    printf 'R' >&3 2>/dev/null || true
  fi
  trim_log
  sleep "$PET_INTERVAL"
done
