#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/home/ubuntu/telegram-stremio}"
SERVICE="${SERVICE:-telegram-stremio}"
CONTAINER="${CONTAINER:-tg_stremio}"
IMAGE="${IMAGE:-telegram-stremio:local}"
STREAM_STATS_URL="${STREAM_STATS_URL:-http://127.0.0.1:8000/stream/stats}"
LOGIN_URL="${LOGIN_URL:-http://127.0.0.1:8000/login}"
MANIFEST_URL="${MANIFEST_URL:-}"
STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-180}"

cd "${APP_DIR}"

if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose v2 is required; legacy docker-compose is not supported." >&2
  exit 1
fi

active_streams="$(
  curl -fsS --max-time 10 "${STREAM_STATS_URL}" \
    | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("active_streams", [])))'
)"
if [ "${active_streams}" -ne 0 ]; then
  echo "Deployment refused: ${active_streams} active stream(s)." >&2
  exit 2
fi

expected_version="$(
  python3 - <<'PY'
import pathlib
import re

text = pathlib.Path("Backend/__init__.py").read_text(encoding="utf-8")
match = re.search(r'__version__\s*=\s*"([^"]+)"', text)
if not match:
    raise SystemExit("Could not read project version")
print(match.group(1))
PY
)"

docker compose build "${SERVICE}"

built_image_id="$(docker image inspect "${IMAGE}" --format '{{.Id}}')"
built_version="$(
  docker run --rm --entrypoint python "${IMAGE}" -c \
    'import Backend; print(Backend.__version__)'
)"
if [ "${built_version}" != "${expected_version}" ]; then
  echo "Built image version mismatch: expected ${expected_version}, got ${built_version}." >&2
  exit 3
fi

docker run --rm --entrypoint python "${IMAGE}" -c \
  'from Backend.helper.metadata_matcher import build_title_variants; assert build_title_variants("Patriot.2026", "Patriot", 2026)'

docker compose up -d --no-deps --force-recreate "${SERVICE}"

deadline=$((SECONDS + STARTUP_TIMEOUT_SECONDS))
until curl -fsS --max-time 10 -o /dev/null "${LOGIN_URL}"; do
  if [ "${SECONDS}" -ge "${deadline}" ]; then
    echo "Application did not become healthy within ${STARTUP_TIMEOUT_SECONDS}s." >&2
    docker logs --tail 100 "${CONTAINER}" >&2 || true
    exit 4
  fi
  sleep 5
done

running_image_id="$(docker inspect "${CONTAINER}" --format '{{.Image}}')"
if [ "${running_image_id}" != "${built_image_id}" ]; then
  echo "Running image mismatch: built ${built_image_id}, running ${running_image_id}." >&2
  exit 5
fi

running_version="$(
  docker exec "${CONTAINER}" python -c 'import Backend; print(Backend.__version__)'
)"
if [ "${running_version}" != "${expected_version}" ]; then
  echo "Running version mismatch: expected ${expected_version}, got ${running_version}." >&2
  exit 6
fi

curl -fsS --max-time 10 -o /dev/null "${STREAM_STATS_URL}"
if [ -n "${MANIFEST_URL}" ]; then
  curl -fsS --max-time 15 -o /dev/null "${MANIFEST_URL}"
fi

echo "Deployment verified: version=${running_version} image=${running_image_id}"
