from os import getenv, path
from dotenv import load_dotenv

load_dotenv(path.join(path.dirname(path.dirname(__file__)), "config.env"))

class Telegram:
    API_ID = int(getenv("API_ID", "0"))
    API_HASH = getenv("API_HASH", "")
    BOT_TOKEN = getenv("BOT_TOKEN", "")
    HELPER_BOT_TOKEN = getenv("HELPER_BOT_TOKEN", "")
    TELEGRAM_PROXY_ENABLED = getenv("TELEGRAM_PROXY_ENABLED", "false").lower() == "true"
    TELEGRAM_PROXY_SCHEME = getenv("TELEGRAM_PROXY_SCHEME", "socks5").strip().lower()
    TELEGRAM_PROXY_HOST = getenv("TELEGRAM_PROXY_HOST", "").strip()
    try:
        TELEGRAM_PROXY_PORT = int(getenv("TELEGRAM_PROXY_PORT", "0") or 0)
    except Exception:
        TELEGRAM_PROXY_PORT = 0
    TELEGRAM_PROXY_USERNAME = getenv("TELEGRAM_PROXY_USERNAME", "").strip()
    TELEGRAM_PROXY_PASSWORD = getenv("TELEGRAM_PROXY_PASSWORD", "").strip()
    WARP_CONTROL_COMMAND = getenv("WARP_CONTROL_COMMAND", "").strip()
    WARP_CONTROL_URL = getenv("WARP_CONTROL_URL", "").strip().rstrip("/")
    WARP_CONTROL_SECRET = getenv("WARP_CONTROL_SECRET", "").strip()
    try:
        TELEGRAM_CLIENT_START_TIMEOUT_SEC = int(getenv("TELEGRAM_CLIENT_START_TIMEOUT_SEC", "45") or 45)
    except Exception:
        TELEGRAM_CLIENT_START_TIMEOUT_SEC = 45

    @classmethod
    def telegram_proxy(cls):
        if not cls.TELEGRAM_PROXY_ENABLED:
            return None
        if cls.TELEGRAM_PROXY_SCHEME.upper() not in {"SOCKS4", "SOCKS5", "HTTP"}:
            raise ValueError("TELEGRAM_PROXY_SCHEME must be one of: socks4, socks5, http")
        if not cls.TELEGRAM_PROXY_HOST or not cls.TELEGRAM_PROXY_PORT:
            raise ValueError("TELEGRAM_PROXY_HOST and TELEGRAM_PROXY_PORT are required when TELEGRAM_PROXY_ENABLED=true")
        proxy = {
            "scheme": cls.TELEGRAM_PROXY_SCHEME,
            "hostname": cls.TELEGRAM_PROXY_HOST,
            "port": cls.TELEGRAM_PROXY_PORT,
        }
        if cls.TELEGRAM_PROXY_USERNAME:
            proxy["username"] = cls.TELEGRAM_PROXY_USERNAME
        if cls.TELEGRAM_PROXY_PASSWORD:
            proxy["password"] = cls.TELEGRAM_PROXY_PASSWORD
        return proxy

    BASE_URL = getenv("BASE_URL", "").rstrip('/')
    PORT = int(getenv("PORT", "8000"))

    PARALLEL = int(getenv("PARALLEL", "1"))
    PRE_FETCH = int(getenv("PRE_FETCH", "1"))

    TELEGRAM_CDN_ENABLED = getenv("TELEGRAM_CDN_ENABLED", "true").lower() == "true"
    TELEGRAM_CDN_VERIFY_HASHES = getenv("TELEGRAM_CDN_VERIFY_HASHES", "true").lower() == "true"
    try:
        TELEGRAM_CDN_MAX_REUPLOAD_ATTEMPTS = int(getenv("TELEGRAM_CDN_MAX_REUPLOAD_ATTEMPTS", "2") or 2)
    except Exception:
        TELEGRAM_CDN_MAX_REUPLOAD_ATTEMPTS = 2
    TELEGRAM_CDN_DEBUG_LOGS = getenv("TELEGRAM_CDN_DEBUG_LOGS", "false").lower() == "true"

    SMART_ROUTING_ENABLED = getenv("SMART_ROUTING_ENABLED", "true").lower() == "true"
    SMART_ROUTING_PROBE_ENABLED = getenv("SMART_ROUTING_PROBE_ENABLED", "true").lower() == "true"
    try:
        SMART_ROUTING_PROBE_CLIENTS = int(getenv("SMART_ROUTING_PROBE_CLIENTS", "3") or 3)
    except Exception:
        SMART_ROUTING_PROBE_CLIENTS = 3
    try:
        SMART_ROUTING_PROBE_BYTES = int(getenv("SMART_ROUTING_PROBE_BYTES", "262144") or 262144)
    except Exception:
        SMART_ROUTING_PROBE_BYTES = 262144
    try:
        SMART_ROUTING_PROBE_TIMEOUT_SEC = float(getenv("SMART_ROUTING_PROBE_TIMEOUT_SEC", "4") or 4)
    except Exception:
        SMART_ROUTING_PROBE_TIMEOUT_SEC = 4.0
    try:
        SMART_ROUTING_FIRST_CHUNK_TIMEOUT_SEC = float(getenv("SMART_ROUTING_FIRST_CHUNK_TIMEOUT_SEC", "4") or 4)
    except Exception:
        SMART_ROUTING_FIRST_CHUNK_TIMEOUT_SEC = 4.0
    try:
        SMART_ROUTING_CHUNK_TIMEOUT_SEC = float(getenv("SMART_ROUTING_CHUNK_TIMEOUT_SEC", "15") or 15)
    except Exception:
        SMART_ROUTING_CHUNK_TIMEOUT_SEC = 15.0

    AUTH_CHANNEL = [channel.strip() for channel in (getenv("AUTH_CHANNEL") or "").split(",") if channel.strip()]
    ANIME_CHANNELS = [channel.strip() for channel in (getenv("ANIME_CHANNELS") or "").split(",") if channel.strip()]
    GLOBAL_SEARCH = getenv("GLOBAL_SEARCH", "false").lower() == "true"
    GLOBAL_SEARCH_CHANNELS = [channel.strip() for channel in (getenv("GLOBAL_SEARCH_CHANNELS") or "").split(",") if channel.strip()]
    DATABASE = [db.strip() for db in (getenv("DATABASE") or "").split(",") if db.strip()]

    TMDB_API = getenv("TMDB_API", "")

    # -------------------------------
    # LLM metadata reranker (optional, fast fallback for low-confidence matches)
    # -------------------------------
    METADATA_RERANKER_ENABLED = getenv(
        "METADATA_RERANKER_ENABLED",
        getenv("GEMINI_MATCHER_ENABLED", "false"),
    ).lower() == "true"
    GEMINI_MATCHER_ENABLED = METADATA_RERANKER_ENABLED
    METADATA_RERANKER_PROVIDER = getenv("METADATA_RERANKER_PROVIDER", "auto").strip().lower()
    GEMINI_API_KEY = getenv("GEMINI_API_KEY", "").strip()
    GEMINI_MATCHER_MODEL = getenv("GEMINI_MATCHER_MODEL", "gemini-3.1-flash-lite").strip()
    GEMINI_MATCHER_FALLBACK_MODEL = getenv("GEMINI_MATCHER_FALLBACK_MODEL", "gemini-2.5-flash-lite").strip()
    GROQ_API_KEY = getenv("GROQ_API_KEY", "").strip()
    GROQ_MATCHER_MODEL = getenv("GROQ_MATCHER_MODEL", "llama-3.1-8b-instant").strip()
    GROQ_MATCHER_FALLBACK_MODEL = getenv("GROQ_MATCHER_FALLBACK_MODEL", "llama-3.3-70b-versatile").strip()
    try:
        GEMINI_MATCHER_TIMEOUT_SECONDS = max(0.1, float(getenv("GEMINI_MATCHER_TIMEOUT_SECONDS", "0.9") or 0.9))
    except Exception:
        GEMINI_MATCHER_TIMEOUT_SECONDS = 0.9
    try:
        GEMINI_MATCHER_MAX_CANDIDATES = max(2, min(8, int(getenv("GEMINI_MATCHER_MAX_CANDIDATES", "4") or 4)))
    except Exception:
        GEMINI_MATCHER_MAX_CANDIDATES = 4
    try:
        GEMINI_MATCHER_MIN_TOP_MARGIN = float(getenv("GEMINI_MATCHER_MIN_TOP_MARGIN", "8") or 8)
    except Exception:
        GEMINI_MATCHER_MIN_TOP_MARGIN = 8.0
    try:
        GEMINI_MATCHER_CACHE_TTL_SECONDS = max(0, int(getenv("GEMINI_MATCHER_CACHE_TTL_SECONDS", "86400") or 86400))
    except Exception:
        GEMINI_MATCHER_CACHE_TTL_SECONDS = 86400
    try:
        GEMINI_MATCHER_CACHE_MAX = max(0, int(getenv("GEMINI_MATCHER_CACHE_MAX", "2000") or 2000))
    except Exception:
        GEMINI_MATCHER_CACHE_MAX = 2000

    UPSTREAM_REPO = getenv("UPSTREAM_REPO", "")
    UPSTREAM_BRANCH = getenv("UPSTREAM_BRANCH", "")

    OWNER_ID = int(getenv("OWNER_ID", "5422223708"))
    
    REPLACE_MODE = getenv("REPLACE_MODE", "true").lower() == "true"
    HIDE_CATALOG = getenv("HIDE_CATALOG", "false").lower() == "true"

    AUTO_CATALOG_REGION = getenv("AUTO_CATALOG_REGION", "IN")
    AUTO_CATALOG_ON_STARTUP = getenv("AUTO_CATALOG_ON_STARTUP", "true").lower() == "true"
    AUTO_CATALOG_FULL_REBUILD_ON_STARTUP = getenv("AUTO_CATALOG_FULL_REBUILD_ON_STARTUP", "false").lower() == "true"
    try:
        AUTO_SYNC_DELAY_SECONDS = int(getenv("AUTO_SYNC_DELAY_SECONDS", "20") or 20)
    except Exception:
        AUTO_SYNC_DELAY_SECONDS = 20
    try:
        AUTO_SYNC_CONCURRENCY = int(getenv("AUTO_SYNC_CONCURRENCY", "5") or 5)
    except Exception:
        AUTO_SYNC_CONCURRENCY = 5
    AUTO_CATALOG_INTERVAL_SYNC = getenv("AUTO_CATALOG_INTERVAL_SYNC", "true").lower() == "true"
    try:
        AUTO_CATALOG_SYNC_INTERVAL_MINUTES = int(getenv("AUTO_CATALOG_SYNC_INTERVAL_MINUTES", "60") or 60)
    except Exception:
        AUTO_CATALOG_SYNC_INTERVAL_MINUTES = 60

    # -------------------------------
    # IPTV live TV (iptv-org, global by default)
    # -------------------------------
    IPTV_ENABLED = getenv("IPTV_ENABLED", "true").lower() == "true"
    IPTV_COUNTRY_CODES = [
        code.strip().upper()
        for code in getenv("IPTV_COUNTRY_CODES", "").split(",")
        if code.strip()
    ]
    try:
        IPTV_PAGE_SIZE = max(1, min(100, int(getenv("IPTV_PAGE_SIZE", "50") or 50)))
    except Exception:
        IPTV_PAGE_SIZE = 50
    IPTV_AUTO_SYNC = getenv("IPTV_AUTO_SYNC", "true").lower() == "true"
    try:
        IPTV_SYNC_INTERVAL_MINUTES = max(30, int(getenv("IPTV_SYNC_INTERVAL_MINUTES", "360") or 360))
    except Exception:
        IPTV_SYNC_INTERVAL_MINUTES = 360
    try:
        IPTV_SYNC_START_DELAY_SECONDS = max(0, int(getenv("IPTV_SYNC_START_DELAY_SECONDS", "30") or 30))
    except Exception:
        IPTV_SYNC_START_DELAY_SECONDS = 30
    try:
        IPTV_REQUEST_TIMEOUT_SEC = max(5.0, float(getenv("IPTV_REQUEST_TIMEOUT_SEC", "45") or 45))
    except Exception:
        IPTV_REQUEST_TIMEOUT_SEC = 45.0
    try:
        IPTV_PROXY_TIMEOUT_SEC = max(5.0, float(getenv("IPTV_PROXY_TIMEOUT_SEC", "30") or 30))
    except Exception:
        IPTV_PROXY_TIMEOUT_SEC = 30.0
    IPTV_PROXY_FALLBACK_ENABLED = getenv("IPTV_PROXY_FALLBACK_ENABLED", "true").lower() == "true"
    IPTV_PROXY_SECRET = getenv("IPTV_PROXY_SECRET", "").strip()
    IPTV_API_BASE_URL = getenv("IPTV_API_BASE_URL", "https://iptv-org.github.io/api").rstrip("/")

    ADMIN_USERNAME = getenv("ADMIN_USERNAME", "fyvio")
    ADMIN_PASSWORD = getenv("ADMIN_PASSWORD", "fyvio")
    DEFAULT_ADDON_TOKEN = getenv("DEFAULT_ADDON_TOKEN", "").strip()
    
    SUBSCRIPTION = getenv("SUBSCRIPTION", "false").lower() == "true"
    SUBSCRIPTION_GROUP_ID = int(getenv("SUBSCRIPTION_GROUP_ID", "0"))
    SUBSCRIPTION_URL = getenv("SUBSCRIPTION_URL", "https://t.me/")
    APPROVER_IDS = [int(x.strip()) for x in (getenv("APPROVER_IDS") or "").split(",") if x.strip().isdigit()]

    PROXY = getenv("Proxy", "false").lower() == "true"
    PROXY_TYPE = getenv("ProxyType", "HTTPS")
    HTTP_PROXY_URL = getenv("HTTP_Proxy_URL", "")
    SHOW_PROXY_AND_NON_PROXY_BOTH = getenv("SHOW_ProxyAndNonProxyBoth", "false").lower() == "true"

    # -------------------------------
    # Disk cache + nginx offload (optional)
    # -------------------------------
    DISK_CACHE_ENABLED = getenv("DISK_CACHE_ENABLED", "false").lower() == "true"
    DISK_CACHE_DIR = getenv("DISK_CACHE_DIR", "cache")
    try:
        DISK_CACHE_MAX_GB = float(getenv("DISK_CACHE_MAX_GB", "0") or 0)
    except Exception:
        DISK_CACHE_MAX_GB = 0.0
    try:
        DISK_CACHE_MAX_BYTES = int(getenv("DISK_CACHE_MAX_BYTES", "0") or 0)
    except Exception:
        DISK_CACHE_MAX_BYTES = 0

    DISK_CACHE_CONCURRENCY = int(getenv("DISK_CACHE_CONCURRENCY", "1") or 1)
    DISK_CACHE_PRECACHE_ON_INGEST = getenv("DISK_CACHE_PRECACHE_ON_INGEST", "false").lower() == "true"

    NGINX_ACCEL_REDIRECT_ENABLED = getenv("NGINX_ACCEL_REDIRECT_ENABLED", "false").lower() == "true"
    NGINX_ACCEL_REDIRECT_LOCATION = getenv("NGINX_ACCEL_REDIRECT_LOCATION", "/_cache/")

    # -------------------------------
    # Streaming SLO warnings (logs only)
    # -------------------------------
    try:
        STREAM_SLO_TTFB_WARN_SEC = float(getenv("STREAM_SLO_TTFB_WARN_SEC", "3") or 3)
    except Exception:
        STREAM_SLO_TTFB_WARN_SEC = 3.0
    try:
        STREAM_SLO_TIMEOUT_WARN_COUNT = int(getenv("STREAM_SLO_TIMEOUT_WARN_COUNT", "2") or 2)
    except Exception:
        STREAM_SLO_TIMEOUT_WARN_COUNT = 2
    try:
        STREAM_SLO_BUFFERING_WARN_RATE = float(getenv("STREAM_SLO_BUFFERING_WARN_RATE", "0.05") or 0.05)
    except Exception:
        STREAM_SLO_BUFFERING_WARN_RATE = 0.05

    # -------------------------------
    # Adaptive Telegram stream safety
    # -------------------------------
    ADAPTIVE_PREFETCH_ENABLED = getenv("ADAPTIVE_PREFETCH_ENABLED", "true").lower() == "true"
    try:
        ADAPTIVE_PREFETCH_LOW_MEM_MB = int(getenv("ADAPTIVE_PREFETCH_LOW_MEM_MB", "150") or 150)
    except Exception:
        ADAPTIVE_PREFETCH_LOW_MEM_MB = 150
    try:
        ADAPTIVE_PREFETCH_MULTI_STREAM_THRESHOLD = int(getenv("ADAPTIVE_PREFETCH_MULTI_STREAM_THRESHOLD", "2") or 2)
    except Exception:
        ADAPTIVE_PREFETCH_MULTI_STREAM_THRESHOLD = 2
    try:
        ADAPTIVE_PREFETCH_SMALL_REQUEST_BYTES = int(getenv("ADAPTIVE_PREFETCH_SMALL_REQUEST_BYTES", str(16 * 1024 * 1024)) or 16 * 1024 * 1024)
    except Exception:
        ADAPTIVE_PREFETCH_SMALL_REQUEST_BYTES = 16 * 1024 * 1024
    try:
        ADAPTIVE_PREFETCH_SMALL_FILE_BYTES = int(getenv("ADAPTIVE_PREFETCH_SMALL_FILE_BYTES", str(64 * 1024 * 1024)) or 64 * 1024 * 1024)
    except Exception:
        ADAPTIVE_PREFETCH_SMALL_FILE_BYTES = 64 * 1024 * 1024

    try:
        SMART_ROUTING_COOLDOWN_FAILURES = int(getenv("SMART_ROUTING_COOLDOWN_FAILURES", "2") or 2)
    except Exception:
        SMART_ROUTING_COOLDOWN_FAILURES = 2
    try:
        SMART_ROUTING_COOLDOWN_SEC = int(getenv("SMART_ROUTING_COOLDOWN_SEC", "180") or 180)
    except Exception:
        SMART_ROUTING_COOLDOWN_SEC = 180

    # -------------------------------
    # Torrent tracker scrape stats (optional, lightweight)
    # -------------------------------
    TORRENT_STATS_ENABLED = getenv("TORRENT_STATS_ENABLED", "true").lower() == "true"
    try:
        TORRENT_STATS_TTL_SEC = int(getenv("TORRENT_STATS_TTL_SEC", "21600") or 21600)
    except Exception:
        TORRENT_STATS_TTL_SEC = 21600
    try:
        TORRENT_STATS_FAILURE_TTL_SEC = int(getenv("TORRENT_STATS_FAILURE_TTL_SEC", "3600") or 3600)
    except Exception:
        TORRENT_STATS_FAILURE_TTL_SEC = 3600
    try:
        TORRENT_STATS_MAX_TRACKERS = int(getenv("TORRENT_STATS_MAX_TRACKERS", "5") or 5)
    except Exception:
        TORRENT_STATS_MAX_TRACKERS = 5
    try:
        TORRENT_STATS_TIMEOUT_SEC = float(getenv("TORRENT_STATS_TIMEOUT_SEC", "2.5") or 2.5)
    except Exception:
        TORRENT_STATS_TIMEOUT_SEC = 2.5
    try:
        TORRENT_STATS_CONCURRENCY = int(getenv("TORRENT_STATS_CONCURRENCY", "3") or 3)
    except Exception:
        TORRENT_STATS_CONCURRENCY = 3

    # -------------------------------
    # Torrent download-to-VPS cache (manual)
    # -------------------------------
    TORRENT_DOWNLOADS_ENABLED = getenv("TORRENT_DOWNLOADS_ENABLED", "true").lower() == "true"
    TORRENT_DOWNLOAD_ROOT = getenv("TORRENT_DOWNLOAD_ROOT", "/downloads/completed")
    try:
        TORRENT_DOWNLOAD_MIN_FREE_GB = float(getenv("TORRENT_DOWNLOAD_MIN_FREE_GB", "10") or 10)
    except Exception:
        TORRENT_DOWNLOAD_MIN_FREE_GB = 10.0
    try:
        TORRENT_DOWNLOAD_CONCURRENCY = int(getenv("TORRENT_DOWNLOAD_CONCURRENCY", "1") or 1)
    except Exception:
        TORRENT_DOWNLOAD_CONCURRENCY = 1
    try:
        TORRENT_DOWNLOAD_POLL_SEC = int(getenv("TORRENT_DOWNLOAD_POLL_SEC", "15") or 15)
    except Exception:
        TORRENT_DOWNLOAD_POLL_SEC = 15
    try:
        TORRENT_DOWNLOAD_PROGRESS_EDIT_SEC = int(getenv("TORRENT_DOWNLOAD_PROGRESS_EDIT_SEC", "60") or 60)
    except Exception:
        TORRENT_DOWNLOAD_PROGRESS_EDIT_SEC = 60
    try:
        TORRENT_DOWNLOAD_STALL_TIMEOUT_SEC = int(getenv("TORRENT_DOWNLOAD_STALL_TIMEOUT_SEC", "3600") or 3600)
    except Exception:
        TORRENT_DOWNLOAD_STALL_TIMEOUT_SEC = 3600
    try:
        TORRENT_DOWNLOAD_MAX_RUNTIME_SEC = int(getenv("TORRENT_DOWNLOAD_MAX_RUNTIME_SEC", "172800") or 172800)
    except Exception:
        TORRENT_DOWNLOAD_MAX_RUNTIME_SEC = 172800

    QBITTORRENT_BASE_URL = getenv("QBITTORRENT_BASE_URL", "http://qbittorrent:8080").rstrip("/")
    QBITTORRENT_USERNAME = getenv("QBITTORRENT_USERNAME", "")
    QBITTORRENT_PASSWORD = getenv("QBITTORRENT_PASSWORD", "")
    QBITTORRENT_SAVE_PATH = getenv("QBITTORRENT_SAVE_PATH", "/downloads/completed")
    QBITTORRENT_TEMP_PATH = getenv("QBITTORRENT_TEMP_PATH", "/downloads/incomplete")

    NGINX_DOWNLOAD_ACCEL_REDIRECT_ENABLED = getenv("NGINX_DOWNLOAD_ACCEL_REDIRECT_ENABLED", "true").lower() == "true"
    NGINX_DOWNLOAD_ACCEL_REDIRECT_LOCATION = getenv("NGINX_DOWNLOAD_ACCEL_REDIRECT_LOCATION", "/_downloads/")

    # -------------------------------
    # Dashboard egress reporting
    # -------------------------------
    NGINX_EGRESS_ENABLED = getenv("NGINX_EGRESS_ENABLED", "true").lower() == "true"
    NGINX_EGRESS_LOG_PATHS = [
        p.strip()
        for p in getenv(
            "NGINX_EGRESS_LOG_PATHS",
            "/host/var/log/nginx/access.log,/host/var/log/nginx/access.log.1",
        ).split(",")
        if p.strip()
    ]
    NGINX_EGRESS_STREAM_PREFIXES = [
        p.strip()
        for p in getenv("NGINX_EGRESS_STREAM_PREFIXES", "/dl/,/downloaded/").split(",")
        if p.strip()
    ]
    try:
        NGINX_EGRESS_CACHE_SEC = int(getenv("NGINX_EGRESS_CACHE_SEC", "30") or 30)
    except Exception:
        NGINX_EGRESS_CACHE_SEC = 30

    VPS_OUTBOUND_ENABLED = getenv("VPS_OUTBOUND_ENABLED", "true").lower() == "true"
    VPS_OUTBOUND_INTERFACE = getenv("VPS_OUTBOUND_INTERFACE", "ens3")
    VPS_OUTBOUND_TX_BYTES_PATH = getenv("VPS_OUTBOUND_TX_BYTES_PATH", "/host/ens3_tx_bytes")
    VPS_OUTBOUND_NET_DEV_PATH = getenv("VPS_OUTBOUND_NET_DEV_PATH", "/host/proc/net/dev")
    try:
        VPS_OUTBOUND_MONTHLY_LIMIT_BYTES = int(getenv("VPS_OUTBOUND_MONTHLY_LIMIT_BYTES", str(10 * 1024 ** 4)) or 10 * 1024 ** 4)
    except Exception:
        VPS_OUTBOUND_MONTHLY_LIMIT_BYTES = 10 * 1024 ** 4
