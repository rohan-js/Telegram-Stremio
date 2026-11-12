import json
import re
import zlib
from datetime import datetime

from bson import ObjectId

from Backend import __version__, db
from Backend.helper.encrypt import decode_string
from Backend.helper.settings_manager import SettingsManager

_SETTINGS_EXCLUDE = {"admin_password", "session_secret"}
_COLLECTIONS = {
    "custom_catalogs": "custom_catalogs",
    "subscription_plans": "sub_plans",
    "tokens": "api_tokens",
    "requests": "requests",
}
_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def _jsonify(value):
    if isinstance(value, dict):
        return {k: _jsonify(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonify(v) for v in value]
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _revive(value):
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key == "_id" and isinstance(item, str) and len(item) == 24:
                try:
                    out[key] = ObjectId(item)
                    continue
                except Exception:
                    pass
            out[key] = _revive(item)
        return out
    if isinstance(value, list):
        return [_revive(v) for v in value]
    if isinstance(value, str) and _ISO_RE.match(value):
        try:
            return datetime.fromisoformat(value.replace("Z", ""))
        except Exception:
            return value
    return value


async def export_config() -> dict:
    settings = {
        key: value
        for key, value in SettingsManager.current().to_dict().items()
        if key not in _SETTINGS_EXCLUDE
    }
    payload = {
        "app": "telegram-stremio",
        "version": __version__,
        "exported_at": datetime.utcnow().isoformat(),
        "settings": settings,
    }
    for label, collection in _COLLECTIONS.items():
        payload[label] = await db.dbs["tracking"][collection].find({}).to_list(None)
    return _jsonify(payload)


async def import_config(payload: dict) -> dict:
    if not isinstance(payload, dict) or payload.get("app") != "telegram-stremio":
        raise ValueError("Invalid Telegram-Stremio backup file.")
    result = {}
    settings = payload.get("settings")
    if isinstance(settings, dict):
        clean = {k: v for k, v in settings.items() if k not in _SETTINGS_EXCLUDE and k != "_id"}
        if clean:
            result["settings"] = await SettingsManager.update(db, clean)
    for label, collection in _COLLECTIONS.items():
        docs = payload.get(label)
        if not isinstance(docs, list):
            continue
        revived = [_revive(doc) for doc in docs if isinstance(doc, dict)]
        coll = db.dbs["tracking"][collection]
        await coll.delete_many({})
        if revived:
            await coll.insert_many(revived)
        result[label] = len(revived)
    return result


#----- Library export in TeleStremio's catalog format (gzip, streamed doc by
#----- doc — upstream 3326dec). Stream ids are decoded into chat/message parts
#----- so the TeleStremio app can play them without our addon.
_UNITS = {"B": 1, "KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40}


def _size_bytes(size) -> int:
    m = re.match(r"([\d.]+)\s*([KMGT]?B)", str(size or "").upper())
    return int(float(m.group(1)) * _UNITS[m.group(2)]) if m else 0


async def _quality(q: dict):
    parts = q.get("parts") or []
    if not parts:
        try:
            ref = await decode_string(q.get("id") or "")
            parts = [{"part_number": 0, "chat_id": ref["chat_id"], "msg_id": ref["msg_id"], "size_bytes": _size_bytes(q.get("size"))}]
        except Exception:
            return None
    parts = [{k: int(p.get(k) or 0) for k in ("part_number", "chat_id", "msg_id", "size_bytes")} for p in parts]
    return {
        "quality": q.get("quality") or "HD", "id": "", "name": q.get("name") or "", "size": q.get("size") or "",
        "size_bytes": sum(p["size_bytes"] for p in parts) or _size_bytes(q.get("size")),
        "group_key": q.get("group_key"), "parts": parts,
    }


async def _qualities(items) -> list:
    return [q for q in [await _quality(i) for i in items or []] if q]


async def _library_doc(doc: dict, kind: str) -> dict:
    d = _jsonify({k: v for k, v in doc.items() if k not in ("_id", "updated_on")})
    if isinstance(doc.get("updated_on"), datetime):
        d["updated_on"] = d["added_on"] = int(doc["updated_on"].timestamp() * 1000)
    if kind == "movie":
        d["telegram"] = await _qualities(doc.get("telegram"))
    else:
        for season in d.get("seasons") or []:
            for ep in season.get("episodes") or []:
                ep["telegram"] = await _qualities(ep.get("telegram"))
    return d


async def export_library():
    s = SettingsManager.current().to_dict()
    anime = {str(c) for c in s.get("anime_channels") or []}
    # Fork: no channel_titles setting exists here — TeleStremio titles stay
    # empty rather than guessing from channel usernames.
    titles = {}
    gz = zlib.compressobj(6, zlib.DEFLATED, 31)
    yield gz.compress(b'{"format":"telestremio-catalog","version":2,"source":"telegram-stremio"')
    for kind in ("movie", "tv"):
        yield gz.compress(f',"{kind}":['.encode())
        sep = ""
        for key, store in db.dbs.items():
            if not key.startswith("storage_"):
                continue
            async for doc in store[kind].find({}):
                yield gz.compress((sep + json.dumps(await _library_doc(doc, kind))).encode())
                sep = ","
        yield gz.compress(b"]")
    channels = [
        {"ref": str(c), "anime": str(c) in anime, "title": titles.get(str(c), ""),
         "chat_id": int(c) if str(c).lstrip("-").isdigit() else 0}
        for c in s.get("auth_channels") or []
    ]
    yield gz.compress(f',"channels":{json.dumps(channels)}}}'.encode())
    yield gz.flush()
