import re
from pathlib import Path

from Backend.config import Telegram

LOG_PATHS = [
    Path("botlog.txt"),
    Path("logs/bot.log"),
    Path("Backend/botlog.txt"),
]

_SECRET_PATTERNS = [
    re.compile(r"\b\d{8,12}:[A-Za-z0-9_-]{30,}\b"),
    re.compile(r"mongodb(\+srv)?://[^\s\"']+", re.IGNORECASE),
    re.compile(r"(GEMINI|GROQ|DUCKDNS|TOKEN|SECRET|PASSWORD|SESSION)[_A-Z]*=([^\s\"']+)", re.IGNORECASE),
]


def redact_log_text(text: str) -> str:
    redacted = text or ""
    for pattern in _SECRET_PATTERNS:
        redacted = pattern.sub(lambda match: match.group(0).split("=", 1)[0] + "=<redacted>" if "=" in match.group(0) else "<redacted>", redacted)
    for value in (
        getattr(Telegram, "BOT_TOKEN", ""),
        getattr(Telegram, "HELPER_BOT_TOKEN", ""),
        getattr(Telegram, "DATABASE", ""),
        getattr(Telegram, "GEMINI_API_KEY", ""),
        getattr(Telegram, "GROQ_API_KEY", ""),
        getattr(Telegram, "SESSION_SECRET", ""),
    ):
        if value:
            redacted = redacted.replace(str(value), "<redacted>")
    return redacted


def read_recent_logs(max_bytes: int = 200_000) -> dict:
    for path in LOG_PATHS:
        if path.exists() and path.is_file():
            with path.open("rb") as handle:
                handle.seek(0, 2)
                size = handle.tell()
                handle.seek(max(0, size - max_bytes))
                data = handle.read().decode("utf-8", errors="replace")
            return {"path": str(path), "text": redact_log_text(data), "bytes": len(data.encode("utf-8", errors="ignore"))}
    return {"path": "", "text": "", "bytes": 0}
