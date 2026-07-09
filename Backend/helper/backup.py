import re
from datetime import datetime

from bson import ObjectId

from Backend import __version__, db
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
