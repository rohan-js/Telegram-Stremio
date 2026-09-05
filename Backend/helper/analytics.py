import ipaddress
import socket
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse

import httpx

from Backend import db
from Backend.helper.custom_dl import ACTIVE_STREAMS
from Backend.logger import LOGGER

_IP_CACHE = {}
_IP_TTL = 6 * 3600
_LAST_FULL = {}
_FULL_INTERVAL = 60
#----- A user is "online" only while video bytes are actively flowing
#----- (or within ONLINE_PLAY_WINDOW seconds of the last delivered chunk).
#----- 25s = near real-time without flickering between player chunk requests.
ONLINE_PLAY_WINDOW = 25

#----- Server-own traffic (watchdog checks, organic-admin, host-side tests)
#----- must never pollute real user activity.
_SELF_IPS = set()
_SELF_IPS_FETCH_AT = 0.0
_SELF_IPS_TTL = 3600


def _own_public_ips() -> set:
    global _SELF_IPS, _SELF_IPS_FETCH_AT
    now = time.time()
    if _SELF_IPS and now - _SELF_IPS_FETCH_AT < _SELF_IPS_TTL:
        return _SELF_IPS
    try:
        from Backend.config import Telegram as _TG
        host = urlparse(getattr(_TG, "BASE_URL", "") or "").hostname
        if host:
            _SELF_IPS = set(socket.gethostbyname_ex(host)[2])
    except Exception as e:
        LOGGER.warning(f"[ANALYTICS] self-IP resolution failed (fail-open): {e}")
    _SELF_IPS_FETCH_AT = now
    return _SELF_IPS


def _is_self_ip(ip: str) -> bool:
    if not ip:
        return False
    try:
        addr = ipaddress.ip_address(ip)
        if addr.is_private or addr.is_loopback or addr.is_link_local:
            return True
    except ValueError:
        return False
    return ip in _own_public_ips()

#----- App/device parsed from the ADDON-PROTOCOL User-Agent (manifest/stream requests),
#----- not the video-fetch UA which players spoof.
_APP_MAP = [
    ("nuvio", "Nuvio"),
    ("stremio", "Stremio"),
    ("vidi", "Vidi"),
    ("jellyfin", "Jellyfin"),
    ("emby", "Emby"),
    ("plex", "Plex"),
    ("infuse", "Infuse"),
    ("outplayer", "Outplayer"),
    ("iina", "IINA"),
    ("vlc", "VLC"),
    ("mpv", "mpv"),
    ("kodi", "Kodi"),
    ("xbmc", "Kodi"),
    ("mxplayer", "MX Player"),
    ("mxtech", "MX Player"),
    ("exoplayer", "ExoPlayer"),
    ("media3", "ExoPlayer"),
    ("applecoremedia", "Apple Player"),
    ("lavf", "FFmpeg / .strm"),
    ("ffmpeg", "FFmpeg / .strm"),
    ("libav", "FFmpeg / .strm"),
    ("okhttp", "Android App"),
    ("dalvik", "Android App"),
    ("ktor", "App"),
    ("cfnetwork", "iOS App"),
    ("edg", "Edge"),
    ("opr", "Opera"),
    ("firefox", "Firefox"),
    ("chrome", "Chrome"),
    ("safari", "Safari"),
    ("mozilla", "Browser"),
]

_DEVICE_MAP = [
    ("android tv", "Android TV"),
    ("androidtv", "Android TV"),
    ("googletv", "Android TV"),
    ("google tv", "Android TV"),
    ("bravia", "Android TV"),
    ("shield", "Android TV"),
    ("aft", "Fire TV"),
    ("appletv", "Apple TV"),
    ("apple tv", "Apple TV"),
    ("tvos", "Apple TV"),
    ("tizen", "Samsung TV"),
    ("web0s", "LG TV"),
    ("webos", "LG TV"),
    ("roku", "Roku"),
    ("smarttv", "Smart TV"),
    ("smart-tv", "Smart TV"),
    ("crkey", "Chromecast"),
    ("ipad", "iPad"),
    ("iphone", "iPhone"),
    ("ipod", "iPhone"),
    ("android", "Android"),
    ("windows nt", "Windows"),
    ("macintosh", "macOS"),
    ("mac os x", "macOS"),
    ("cros", "ChromeOS"),
    ("linux", "Linux"),
]


#----- Real client IP behind Cloudflare / Caddy / reverse proxies
def client_ip_from(request) -> str:
    for h in ("cf-connecting-ip", "x-real-ip"):
        v = request.headers.get(h)
        if v:
            return v.strip()
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else ""


def parse_app(user_agent: str) -> str:
    if not user_agent:
        return "Unknown"
    low = user_agent.lower()
    for needle, name in _APP_MAP:
        if needle in low:
            return name
    return "Unknown"


def parse_device(user_agent: str) -> str:
    if not user_agent:
        return ""
    low = user_agent.lower()
    for needle, name in _DEVICE_MAP:
        if needle in low:
            return name
    return ""


async def lookup_ip(ip: str) -> dict:
    if not ip or ip.startswith(("127.", "10.", "192.168.", "172.")) or ip in ("::1", "localhost"):
        return {"country": "Local", "country_code": "", "city": "", "isp": "", "proxy": False}
    now = time.time()
    cached = _IP_CACHE.get(ip)
    if cached and now - cached[1] < _IP_TTL:
        return cached[0]
    data = {"country": "", "country_code": "", "city": "", "isp": "", "proxy": False}
    try:
        async with httpx.AsyncClient(timeout=6) as client:
            resp = await client.get(
                f"http://ip-api.com/json/{ip}",
                params={"fields": "status,country,countryCode,city,isp,proxy,hosting"},
            )
            if resp.status_code == 200:
                j = resp.json()
                if j.get("status") == "success":
                    data = {
                        "country": j.get("country") or "",
                        "country_code": j.get("countryCode") or "",
                        "city": j.get("city") or "",
                        "isp": j.get("isp") or "",
                        "proxy": bool(j.get("proxy") or j.get("hosting")),
                    }
    except Exception as e:
        LOGGER.warning(f"[ANALYTICS] IP lookup failed for {ip}: {e}")
    _IP_CACHE[ip] = (data, now)
    return data


async def _record(token: str, name: str, ip: str, user_agent: str, is_client: bool) -> None:
    if not token:
        return
    #----- Exclude server-own traffic (watchdog / organic-admin / host tests).
    if _is_self_ip(ip):
        return
    coll = db.dbs["tracking"]["user_activity"]
    now = datetime.utcnow()
    setf = {"last_active": now, "ip": ip or ""}
    #----- Only the addon-protocol request carries a trustworthy app/device UA.
    if is_client:
        setf["app"] = parse_app(user_agent)
        setf["device"] = parse_device(user_agent)
        setf["user_agent"] = user_agent or ""
    else:
        #----- Actual video byte-stream: refresh the playback-presence window.
        setf["last_play_active"] = now
    try:
        await coll.update_one(
            {"_id": token},
            {"$set": setf, "$setOnInsert": {"name": name or "Unknown"}},
            upsert=True,
        )
    except Exception as e:
        LOGGER.warning(f"[ANALYTICS] activity ping failed: {e}")
        return

    #----- The IP geo/ISP/VPN lookup is the only slow part — throttle it per token.
    now_ts = time.time()
    if now_ts - _LAST_FULL.get(token, 0) < _FULL_INTERVAL:
        return
    _LAST_FULL[token] = now_ts

    geo = await lookup_ip(ip)
    try:
        await coll.update_one({"_id": token}, {"$set": {
            "country": geo.get("country"),
            "country_code": geo.get("country_code"),
            "city": geo.get("city"),
            "isp": geo.get("isp"),
            "proxy": geo.get("proxy", False),
        }})
    except Exception as e:
        LOGGER.warning(f"[ANALYTICS] geo update failed: {e}")


#----- Called from the video byte-stream (/dl/): only refreshes presence, not device.
async def record_stream_start(token: str, name: str, ip: str, user_agent: str = "") -> None:
    await _record(token, name, ip, user_agent, is_client=False)


#----- Called from the addon protocol (stream/manifest): captures the real app/device.
async def record_client(token: str, name: str, ip: str, user_agent: str = "") -> None:
    await _record(token, name, ip, user_agent, is_client=True)


async def get_activity_overview(page: int = 1, per_page: int = 12) -> dict:
    now = datetime.utcnow()
    now_ts = time.time()
    cutoff = now - timedelta(seconds=ONLINE_PLAY_WINDOW)
    coll = db.dbs["tracking"]["user_activity"]
    stream_coll = db.dbs["tracking"]["stream_analytics"]

    #----- Valid tokens only: revoked/pruned tokens must not linger as ghost
    #----- activity rows (orphan cleanup ported from upstream v5.0.2).
    valid_tokens = set()
    try:
        async for doc in db.dbs["tracking"]["api_tokens"].find({}, {"token": 1}):
            tok = doc.get("token")
            if tok:
                valid_tokens.add(tok)
    except Exception:
        valid_tokens = set()
    if valid_tokens:
        try:
            await coll.delete_many({"_id": {"$nin": list(valid_tokens)}})
        except Exception:
            pass

    #----- Real-time: a token is "playing" only while its stream entry is
    #----- actively delivering bytes (entry["last_ts"] refreshes per chunk).
    playing = {}
    playing_fresh_tokens = set()
    playing_fresh = 0
    for info in ACTIVE_STREAMS.values():
        meta = info.get("meta", {}) or {}
        tok = meta.get("token")
        if not tok:
            continue
        if valid_tokens and tok not in valid_tokens:
            continue
        playing[tok] = meta.get("title") or "Streaming"
        last_ts = info.get("last_ts") or info.get("start_ts") or now_ts
        if now_ts - last_ts <= ONLINE_PLAY_WINDOW:
            playing_fresh_tokens.add(tok)
            playing_fresh += 1

    base_match = {"_id": {"$in": list(valid_tokens)}} if valid_tokens else {}
    try:
        total = await coll.count_documents(base_match)
        online_count = await coll.count_documents({**base_match, "last_play_active": {"$gte": cutoff}})
    except Exception:
        total, online_count = 0, 0
    online_count = max(online_count, playing_fresh)

    #----- Streams actually played in the last 24h (per display name).
    streams24h = {}
    try:
        ago = now - timedelta(hours=24)
        pipeline = [
            {"$match": {"logged_at": {"$gte": ago}}},
            {"$group": {"_id": "$user_name", "count": {"$sum": 1}}},
        ]
        async for group in stream_coll.aggregate(pipeline):
            streams24h[group["_id"]] = int(group.get("count") or 0)
    except Exception:
        streams24h = {}

    #----- Live-only view: the card shows ONLY users who are streaming now.
    #----- Stale historical rows are never listed (empty state instead).
    per_page = max(1, min(int(per_page or 12), 60))
    online_tokens = list(playing_fresh_tokens)
    try:
        live_clause = (
            {"$or": [{"last_play_active": {"$gte": cutoff}}, {"_id": {"$in": online_tokens}}]}
            if online_tokens
            else {"last_play_active": {"$gte": cutoff}}
        )
        query = {"$and": [live_clause, base_match]} if base_match else live_clause
        docs = await coll.find(query).sort("last_active", -1).limit(100).to_list(100)
    except Exception:
        docs = []

    rows = []
    for d in docs:
        token = d.get("_id")
        last_play = d.get("last_play_active")
        online = token in playing_fresh_tokens
        if not online and last_play:
            try:
                online = last_play >= cutoff
            except Exception:
                online = False
        if not online:
            continue
        rows.append({
            "token": token,
            "name": d.get("name") or "Unknown",
            "online": online,
            "now_playing": playing.get(token),
            "last_title": d.get("last_title"),
            "ip": d.get("ip") or "",
            "country": d.get("country") or "",
            "country_code": (d.get("country_code") or "").upper(),
            "city": d.get("city") or "",
            "isp": d.get("isp") or "",
            "app": d.get("app") or "Unknown",
            "device": d.get("device") or "",
            "proxy": bool(d.get("proxy")),
            "streams": int(d.get("streams") or 0),
            "streams24h": int(streams24h.get(d.get("name") or "") or 0),
            "last_active": (d.get("last_active") or last_play).isoformat() if (d.get("last_active") or last_play) else None,
            "last_play_active": last_play.isoformat() if last_play else None,
        })

    rows.sort(key=lambda r: 0 if r["online"] else 1)
    total = len(rows)
    total_pages = 1
    page = 1
    return {
        "users": rows,
        "online_count": online_count,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }
