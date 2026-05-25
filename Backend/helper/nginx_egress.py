import gzip
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit

from Backend.config import Telegram
from Backend.logger import LOGGER


ACCESS_LOG_RE = re.compile(
    r'\[(?P<ts>[^\]]+)\]\s+"(?P<method>[A-Z]+)\s+(?P<uri>\S+)\s+HTTP/[0-9.]+"\s+'
    r"(?P<status>\d{3})\s+(?P<body_bytes>\d+|-)"
)

_CACHE = {"expires_at": 0.0, "data": None}


def _empty_egress_summary(status: str = "unavailable", error: str | None = None) -> dict:
    today = datetime.now().date().isoformat()
    month = today[:7]
    summary = {
        "enabled": bool(Telegram.NGINX_EGRESS_ENABLED),
        "status": status,
        "today": {"date": today, "bytes": 0},
        "month": {"month": month, "bytes": 0},
        "retained": {"bytes": 0},
        "paths": {},
        "log_files": [],
        "source": "nginx access logs",
    }
    if error:
        summary["error"] = error
    return summary


def _open_log(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def _iter_existing_logs(paths: Iterable[str]) -> list[Path]:
    return [Path(path) for path in paths if path and Path(path).is_file()]


def parse_nginx_access_line(line: str) -> dict | None:
    match = ACCESS_LOG_RE.search(line)
    if not match:
        return None

    try:
        logged_at = datetime.strptime(match.group("ts"), "%d/%b/%Y:%H:%M:%S %z")
    except ValueError:
        return None

    raw_bytes = match.group("body_bytes")
    try:
        body_bytes = int(raw_bytes) if raw_bytes != "-" else 0
    except ValueError:
        body_bytes = 0

    try:
        status = int(match.group("status"))
    except ValueError:
        status = 0

    uri = match.group("uri")
    path = urlsplit(uri).path or uri
    return {
        "logged_at": logged_at,
        "method": match.group("method"),
        "path": path,
        "status": status,
        "body_bytes": body_bytes,
    }


def summarize_nginx_egress_logs(
    log_paths: Iterable[str],
    prefixes: Iterable[str],
    now: datetime | None = None,
) -> dict:
    now = now or datetime.now().astimezone()
    today = now.date().isoformat()
    month = today[:7]
    normalized_prefixes = tuple(p for p in prefixes if p)
    existing_logs = _iter_existing_logs(log_paths)

    summary = {
        "enabled": True,
        "status": "ok" if existing_logs else "unavailable",
        "today": {"date": today, "bytes": 0},
        "month": {"month": month, "bytes": 0},
        "retained": {"bytes": 0},
        "paths": {},
        "log_files": [str(path) for path in existing_logs],
        "source": "nginx access logs",
    }

    if not existing_logs:
        summary["error"] = "No configured nginx access logs are readable."
        return summary

    for prefix in normalized_prefixes:
        summary["paths"][prefix] = {"today_bytes": 0, "month_bytes": 0, "retained_bytes": 0}

    for log_path in existing_logs:
        try:
            with _open_log(log_path) as handle:
                for line in handle:
                    parsed = parse_nginx_access_line(line)
                    if not parsed:
                        continue
                    if parsed["status"] not in (200, 206):
                        continue
                    body_bytes = parsed["body_bytes"]
                    if body_bytes <= 0:
                        continue

                    matched_prefix = next(
                        (prefix for prefix in normalized_prefixes if parsed["path"].startswith(prefix)),
                        None,
                    )
                    if not matched_prefix:
                        continue

                    logged_date = parsed["logged_at"].date().isoformat()
                    logged_month = logged_date[:7]
                    path_bucket = summary["paths"].setdefault(
                        matched_prefix,
                        {"today_bytes": 0, "month_bytes": 0, "retained_bytes": 0},
                    )

                    summary["retained"]["bytes"] += body_bytes
                    path_bucket["retained_bytes"] += body_bytes
                    if logged_date == today:
                        summary["today"]["bytes"] += body_bytes
                        path_bucket["today_bytes"] += body_bytes
                    if logged_month == month:
                        summary["month"]["bytes"] += body_bytes
                        path_bucket["month_bytes"] += body_bytes
        except OSError as exc:
            LOGGER.warning("Unable to read nginx egress log %s: %s", log_path, exc)

    return summary


def get_nginx_egress_summary(force: bool = False) -> dict:
    if not Telegram.NGINX_EGRESS_ENABLED:
        return _empty_egress_summary(status="disabled")

    now_ts = time.time()
    cached = _CACHE.get("data")
    if not force and cached and now_ts < float(_CACHE.get("expires_at") or 0):
        return cached

    try:
        summary = summarize_nginx_egress_logs(
            Telegram.NGINX_EGRESS_LOG_PATHS,
            Telegram.NGINX_EGRESS_STREAM_PREFIXES,
        )
    except Exception as exc:
        LOGGER.exception("Unable to summarize nginx egress logs")
        summary = _empty_egress_summary(error=str(exc))

    _CACHE["data"] = summary
    _CACHE["expires_at"] = now_ts + max(0, int(Telegram.NGINX_EGRESS_CACHE_SEC))
    return summary
