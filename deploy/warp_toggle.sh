#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-}"
APP_DIR="${APP_DIR:-/home/ubuntu/telegram-stremio}"
CONFIG_FILE="${CONFIG_FILE:-$APP_DIR/config.env}"
SERVICE="${SERVICE:-telegram-stremio}"
CONTAINER="${CONTAINER:-tg_stremio}"
PROXY_HOST="${TELEGRAM_WARP_PROXY_HOST:-warp_proxy}"
PROXY_PORT="${TELEGRAM_WARP_PROXY_PORT:-40000}"
PROXY_SCHEME="${TELEGRAM_WARP_PROXY_SCHEME:-socks5}"

if [[ "$MODE" != "enable" && "$MODE" != "disable" ]]; then
  echo "usage: $0 enable|disable" >&2
  exit 2
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "config file not found: $CONFIG_FILE" >&2
  exit 1
fi

cd "$APP_DIR"
TS="$(date +%Y%m%d-%H%M%S)"
mkdir -p backups/warp-toggle
cp -a "$CONFIG_FILE" "backups/warp-toggle/config.env.$TS"

compose_cmd=(docker compose)
if [[ -n "${COMPOSE_FILES:-}" ]]; then
  # Space-separated list, for example:
  # COMPOSE_FILES="docker-compose.yaml docker-compose.wgkernel.yml"
  compose_cmd=(docker compose)
  for file in $COMPOSE_FILES; do
    compose_cmd+=(-f "$file")
  done
fi

set_env() {
  local key="$1"
  local value="$2"
  if grep -q "^${key}=" "$CONFIG_FILE"; then
    sed -i "s|^${key}=.*|${key}=\"${value}\"|" "$CONFIG_FILE"
  else
    printf '%s="%s"\n' "$key" "$value" >> "$CONFIG_FILE"
  fi
}

if [[ "$MODE" == "enable" ]]; then
  set_env TELEGRAM_PROXY_ENABLED true
  set_env TELEGRAM_PROXY_SCHEME "$PROXY_SCHEME"
  set_env TELEGRAM_PROXY_HOST "$PROXY_HOST"
  set_env TELEGRAM_PROXY_PORT "$PROXY_PORT"
else
  set_env TELEGRAM_PROXY_ENABLED false
fi

"${compose_cmd[@]}" up -d --no-deps --force-recreate "$SERVICE"

for _ in $(seq 1 40); do
  if curl -fsS -o /dev/null http://127.0.0.1:8000/login; then
    echo "login health ok"
    exit 0
  fi
  sleep 2
done

echo "app did not become healthy after WARP toggle" >&2
docker logs --tail 80 "$CONTAINER" >&2 || true
exit 1
