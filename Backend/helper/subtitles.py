import re
from datetime import datetime
from pathlib import Path

from Backend import db
from Backend.helper.encrypt import encode_string
from Backend.helper.metadata import extract_default_id, metadata
from Backend.helper.pyro import clean_filename
from Backend.logger import LOGGER

SUBTITLE_EXTS = (".srt", ".vtt", ".ass", ".ssa", ".sub")

_LANGUAGES = [
    ("eng", "English", ("english", "eng")),
    ("hin", "Hindi", ("hindi", "hin")),
    ("tam", "Tamil", ("tamil", "tam")),
    ("tel", "Telugu", ("telugu", "tel")),
    ("kan", "Kannada", ("kannada", "kan")),
    ("mal", "Malayalam", ("malayalam", "mal")),
    ("ben", "Bengali", ("bengali", "bangla", "ben")),
    ("mar", "Marathi", ("marathi", "mar")),
    ("pan", "Punjabi", ("punjabi", "panjabi", "pan")),
    ("guj", "Gujarati", ("gujarati", "guj")),
    ("urd", "Urdu", ("urdu", "urd")),
    ("spa", "Spanish", ("spanish", "espanol", "spa")),
    ("fre", "French", ("french", "francais", "fre", "fra")),
    ("ger", "German", ("german", "deutsch", "ger", "deu")),
    ("ita", "Italian", ("italian", "italiano", "ita")),
    ("por", "Portuguese", ("portuguese", "portugues", "por")),
    ("rus", "Russian", ("russian", "rus")),
    ("ara", "Arabic", ("arabic", "ara")),
    ("jpn", "Japanese", ("japanese", "jpn")),
    ("kor", "Korean", ("korean", "kor")),
    ("chi", "Chinese", ("chinese", "mandarin", "cantonese", "chi", "zho")),
]

_LANG_BY_TOKEN = {}
_LANG_WORDS = set()
for code, label, aliases in _LANGUAGES:
    for alias in aliases:
        _LANG_BY_TOKEN.setdefault(alias, (code, label))
        if len(alias) >= 4:
            _LANG_WORDS.add(alias)
    _LANG_WORDS.add(label.lower())

_SUB_EXT_TOKENS = {ext.lstrip(".") for ext in SUBTITLE_EXTS}
_IGNORE_TOKENS = {
    "forced", "sdh", "cc", "full", "default", "hearing", "impaired",
    "dubbed", "dub", "sub", "subs", "subtitle", "subtitles",
}
_TRAILING_LANG_RE = re.compile(
    r"[\s._-]+(" + "|".join(sorted(_LANG_WORDS, key=len, reverse=True)) + r")\s*$",
    re.IGNORECASE,
)


def is_subtitle_file(name: str) -> bool:
    return bool(name) and Path(name.lower().strip()).suffix in SUBTITLE_EXTS


def subtitle_ext(name: str) -> str:
    suffix = Path(name or "").suffix.lower()
    return suffix if suffix in SUBTITLE_EXTS else ".srt"


def detect_language(name: str) -> tuple[str, str]:
    tokens = [t for t in re.split(r"[^a-z0-9]+", (name or "").lower()) if t and t not in _SUB_EXT_TOKENS]
    while tokens and tokens[-1] in _IGNORE_TOKENS:
        tokens.pop()
    for token in reversed(tokens):
        if len(token) >= 4 and token in _LANG_BY_TOKEN:
            return _LANG_BY_TOKEN[token]
    for token in reversed(tokens):
        if len(token) == 3 and token in _LANG_BY_TOKEN:
            return _LANG_BY_TOKEN[token]
    return "und", "Unknown"


def _strip_language_and_ext(name: str) -> str:
    base = name or ""
    ext = subtitle_ext(base)
    if base.lower().endswith(ext):
        base = base[: -len(ext)]
    prev = None
    while prev != base:
        prev = base
        base = _TRAILING_LANG_RE.sub("", base)
    return base.strip() or (name or "")


async def ingest_subtitle(name: str, channel: int, msg_id: int, caption: str | None = None) -> bool:
    try:
        source_text = caption or name
        override_id = extract_default_id(source_text)
        match_name = clean_filename(_strip_language_and_ext(source_text))
        info = await metadata(match_name, int(channel), int(msg_id), override_id=override_id)
        if not info or not info.get("imdb_id"):
            LOGGER.info("[SUBTITLE] Could not match subtitle %s", name)
            return False

        code, label = detect_language(name)
        encoded = await encode_string({"source_type": "subtitle", "chat_id": int(channel), "msg_id": int(msg_id)})
        media_type = "tv" if info.get("media_type") == "tv" else "movie"
        await db.dbs["tracking"]["subtitles"].update_one(
            {"chat_id": int(channel), "msg_id": int(msg_id)},
            {
                "$set": {
                    "imdb_id": info.get("imdb_id"),
                    "tmdb_id": info.get("tmdb_id"),
                    "media_type": media_type,
                    "season": int(info.get("season_number") or 0) or None,
                    "episode": int(info.get("episode_number") or 0) or None,
                    "lang_code": code,
                    "lang_label": label,
                    "name": name,
                    "chat_id": int(channel),
                    "origin_chat_id": int(f"-100{str(channel).replace('-100', '')}"),
                    "msg_id": int(msg_id),
                    "encoded": encoded,
                    "updated_at": datetime.utcnow(),
                },
                "$setOnInsert": {"created_at": datetime.utcnow()},
            },
            upsert=True,
        )
        LOGGER.info("[SUBTITLE] Stored %s subtitle for %s: %s", label, info.get("imdb_id"), name)
        return True
    except Exception as exc:
        LOGGER.error("[SUBTITLE] ingest failed for %s: %s", name, exc)
        return False


async def get_subtitles_for(imdb_id: str, media_type: str, season=None, episode=None) -> list[dict]:
    query = {"imdb_id": imdb_id}
    if media_type in ("tv", "series"):
        query["season"] = int(season) if season else None
        query["episode"] = int(episode) if episode else None
    cursor = db.dbs["tracking"]["subtitles"].find(query).sort("updated_at", -1)
    return await cursor.to_list(None)


async def remove_subtitle(channel: int, msg_id: int) -> bool:
    result = await db.dbs["tracking"]["subtitles"].delete_one(
        {"origin_chat_id": int(channel), "msg_id": int(msg_id)}
    )
    if not result.deleted_count:
        result = await db.dbs["tracking"]["subtitles"].delete_one(
            {"chat_id": int(str(channel).replace("-100", "")), "msg_id": int(msg_id)}
        )
    return result.deleted_count > 0


def stremio_subtitle_entries(subtitles: list[dict], token: str, base_url: str) -> list[dict]:
    entries = []
    for item in subtitles:
        ext = subtitle_ext(item.get("name"))
        encoded = item.get("encoded")
        if not encoded:
            continue
        entries.append(
            {
                "id": f"tg-{item.get('msg_id')}",
                "url": f"{base_url.rstrip()}/sub/{token}/{encoded}/subtitle{ext}",
                "lang": item.get("lang_code") or "und",
            }
        )
    return entries
