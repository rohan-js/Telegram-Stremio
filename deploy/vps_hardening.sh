#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/home/ubuntu/telegram-stremio}"
WATCHDOG_SRC="${WATCHDOG_SRC:-${APP_DIR}/deploy/duckdns_watchdog.sh}"
WATCHDOG_DST="${WATCHDOG_DST:-/home/ubuntu/duckdns_watchdog.sh}"
SWAPFILE="${SWAPFILE:-/swapfile}"
SWAP_SIZE="${SWAP_SIZE:-2G}"
SWAPPINESS="${SWAPPINESS:-10}"
WATCHDOG_CRON="${WATCHDOG_CRON:-*/5 * * * * ${WATCHDOG_DST} >/dev/null 2>&1}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! swapon --show | grep -q "^${SWAPFILE}[[:space:]]"; then
  if [ ! -f "${SWAPFILE}" ]; then
    sudo fallocate -l "${SWAP_SIZE}" "${SWAPFILE}" || sudo dd if=/dev/zero of="${SWAPFILE}" bs=1M count=2048
  fi
  sudo chmod 600 "${SWAPFILE}"
  if ! sudo file "${SWAPFILE}" | grep -q "swap file"; then
    sudo mkswap "${SWAPFILE}" >/dev/null
  fi
  sudo swapon "${SWAPFILE}"
fi

if ! grep -qE "^${SWAPFILE}[[:space:]]+none[[:space:]]+swap" /etc/fstab; then
  printf "%s\n" "${SWAPFILE} none swap sw 0 0" | sudo tee -a /etc/fstab >/dev/null
fi

printf "vm.swappiness=%s\n" "${SWAPPINESS}" | sudo tee /etc/sysctl.d/99-telegram-stremio.conf >/dev/null
sudo sysctl -p /etc/sysctl.d/99-telegram-stremio.conf >/dev/null

if [ -f "${WATCHDOG_SRC}" ]; then
  sudo install -m 755 "${WATCHDOG_SRC}" "${WATCHDOG_DST}"
fi

tmp_cron="$(mktemp)"
(crontab -l 2>/dev/null | grep -v "${WATCHDOG_DST}" || true) > "${tmp_cron}"
printf "%s\n" "${WATCHDOG_CRON}" >> "${tmp_cron}"
crontab "${tmp_cron}"
rm -f "${tmp_cron}"

echo "Hardening applied."
free -h
swapon --show || true
cat /proc/sys/vm/swappiness
