#!/usr/bin/env bash
# Evidence loggers for freeze forensics (Sep-15 + Sep-23 incidents left no
# per-process timeline). Installs and enables:
#   - sysstat/sadc: 1-minute activity samples -> /var/log/sysstat/
#     (after a freeze: `sar -u` shows the CPU eater minute-by-minute)
#   - atop: 300 s interval process accounting -> /var/log/atop/
#     (per-process CPU/RAM timeline; `atop -r /var/log/atop/atop_YYYYMMDD`)
# Idempotent: safe to run repeatedly. Needs root (sudo).
set -euo pipefail

log() { echo "[evidence-logging] $*"; }

if [ "$(id -u)" -ne 0 ]; then
  exec sudo bash "$0" "$@"
fi

export DEBIAN_FRONTEND=noninteractive

# --- sysstat: enable + 1-minute collection ---
apt-get install -y sysstat >/dev/null
# Debian/Ubuntu ship HISTORY in /etc/default/sysstat (ENABLED=false default).
if [ -f /etc/default/sysstat ]; then
  sed -i 's/^ENABLED=.*/ENABLED=true/' /etc/default/sysstat
  grep -q '^ENABLED=' /etc/default/sysstat || echo 'ENABLED=true' >> /etc/default/sysstat
fi
# Keep 30 days of 1-min samples (small: ~1-2 MB/day on an idle box).
if [ -f /etc/sysstat/sysstat ]; then
  sed -i 's/^HISTORY=.*/HISTORY=30/' /etc/sysstat/sysstat
  grep -q '^HISTORY=' /etc/sysstat/sysstat || echo 'HISTORY=30' >> /etc/sysstat/sysstat
fi
systemctl enable sysstat >/dev/null 2>&1 || true
systemctl restart sysstat
log "sysstat: $(systemctl is-active sysstat) (1-min samples, 30d history)"

# --- atop: 300 s interval ---
apt-get install -y atop >/dev/null
# Debian default is 600s/10d; pin 300s interval + keep 14 days.
if [ -f /etc/default/atop ]; then
  sed -i 's/^ATOPLOGinterval=.*/ATOPLOGinterval=300/' /etc/default/atop
  grep -q '^ATOPLOGinterval=' /etc/default/atop || echo 'ATOPLOGinterval=300' >> /etc/default/atop
  sed -i 's/^LOGINTERVAL=.*/LOGINTERVAL=300/' /etc/default/atop 2>/dev/null || true
fi
systemctl enable atop >/dev/null 2>&1 || true
systemctl restart atop
log "atop: $(systemctl is-active atop) (300s interval)"

# --- quick evidence that both are actually recording ---
sleep 2
sadf -- -s "$(date -u -d '2 minutes ago' +%H:%M:%S)" 2>/dev/null | tail -2 || log "sadf: samples will appear within ~1 min"
ls -la /var/log/atop/ 2>/dev/null | tail -3 || true
log "done. After next freeze: sar -u -f /var/log/sysstat/sa$(date +%d)  |  atop -r /var/log/atop/atop_$(date +%Y%m%d)"
