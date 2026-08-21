from os import getenv, path
from dotenv import load_dotenv

load_dotenv(path.join(path.dirname(path.dirname(__file__)), "config.env"))

class Telegram:
    API_ID = int(getenv("API_ID", "0"))
    API_HASH = getenv("API_HASH", "")
    BOT_TOKEN = getenv("BOT_TOKEN", "")
    HELPER_BOT_TOKEN = getenv("HELPER_BOT_TOKEN", "")
    USER_SESSION_STRING = getenv("USER_SESSION_STRING", "").strip()
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
    SESSION_SECRET = getenv(
        "SESSION_SECRET",
        "f6d2e3b9a0f43d9a2e6a56b2d3175cd9c05bbfe31d95ed2a7306b57cb1a8b6f0",
    )

    PARALLEL = int(getenv("PARALLEL", "1"))
    PRE_FETCH = int(getenv("PRE_FETCH", "1"))

    TELEGRAM_CDN_ENABLED = getenv("TELEGRAM_CDN_ENABLED", "false").lower() == "true"
    TELEGRAM_CDN_VERIFY_HASHES = getenv("TELEGRAM_CDN_VERIFY_HASHES", "true").lower() == "true"
    try:
        TELEGRAM_CDN_MAX_REUPLOAD_ATTEMPTS = int(getenv("TELEGRAM_CDN_MAX_REUPLOAD_ATTEMPTS", "2") or 2)
    except Exception:
        TELEGRAM_CDN_MAX_REUPLOAD_ATTEMPTS = 2
    # How many CDN failures for the same file before we stop asking its master DC
    # for CDN redirects and fall back to the plain master-DC upload.getFile path.
    try:
        TELEGRAM_CDN_MAX_FILE_FAILURES = int(getenv("TELEGRAM_CDN_MAX_FILE_FAILURES", "2") or 2)
    except Exception:
        TELEGRAM_CDN_MAX_FILE_FAILURES = 2
    # After this many seconds, a temporarily blacklisted file is retried through CDN again.
    try:
        TELEGRAM_CDN_FILE_DISABLE_TTL_SEC = int(getenv("TELEGRAM_CDN_FILE_DISABLE_TTL_SEC", "300") or 300)
    except Exception:
        TELEGRAM_CDN_FILE_DISABLE_TTL_SEC = 300
    TELEGRAM_CDN_DEBUG_LOGS = getenv("TELEGRAM_CDN_DEBUG_LOGS", "false").lower() == "true"

    SMART_ROUTING_ENABLED = getenv("SMART_ROUTING_ENABLED", "true").lower() == "true"
    SMART_ROUTING_PROBE_ENABLED = getenv("SMART_ROUTING_PROBE_ENABLED", "true").lower() == "true"
    try:
        SMART_ROUTING_PROBE_CLIENTS = int(getenv("SMART_ROUTING_PROBE_CLIENTS", "3") or 3)
    except Exception:
        SMART_ROUTING_PROBE_CLIENTS = 3
    try:
        SMART_ROUTING_PROBE_BYTES = int(getenv("SMART_ROUTING_PROBE_BYTES", "32768") or 32768)
    except Exception:
        SMART_ROUTING_PROBE_BYTES = 32768
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
    try:
        # Skip the live probe when the (client, DC) route produced a successful
        # fetch within this many seconds — repeat opens start without probe lag.
        SMART_ROUTING_PROBE_TRUST_SEC = float(getenv("SMART_ROUTING_PROBE_TRUST_SEC", "60") or 60)
    except Exception:
        SMART_ROUTING_PROBE_TRUST_SEC = 60.0
    try:
        # While the probe runs, stream start begins on the best-known base
        # client. If the probe finishes within this budget its pick is used;
        # beyond it the probe is left running in the background.
        SMART_ROUTING_PROBE_OVERLAP_SEC = float(getenv("SMART_ROUTING_PROBE_OVERLAP_SEC", "0.4") or 0.4)
    except Exception:
        SMART_ROUTING_PROBE_OVERLAP_SEC = 0.4
    try:
        # When a chunk fetch stalls beyond this delay, race a parallel fetch on
        # another healthy helper bot to eliminate micro-freezes.
        SMART_ROUTING_HEDGE_ENABLED = getenv("SMART_ROUTING_HEDGE_ENABLED", "true").lower() == "true"
        SMART_ROUTING_HEDGE_DELAY_SEC = float(getenv("SMART_ROUTING_HEDGE_DELAY_SEC", "0.8") or 0.8)
    except Exception:
        SMART_ROUTING_HEDGE_ENABLED = True
        SMART_ROUTING_HEDGE_DELAY_SEC = 0.8
    try:
        # Pre-warm MTProto sessions to common Telegram DCs at boot and maintain
        # periodic keep-alive pings so cold opens take <1s instead of ~4.8s.
        TELEGRAM_PREWARM_ENABLED = getenv("TELEGRAM_PREWARM_ENABLED", "true").lower() == "true"
        TELEGRAM_PREWARM_DCS = [
            int(x.strip())
            for x in (getenv("TELEGRAM_PREWARM_DCS", "1,2,4,5") or "1,2,4,5").split(",")
            if x.strip().isdigit()
        ]
        TELEGRAM_KEEPALIVE_INTERVAL_SEC = int(getenv("TELEGRAM_KEEPALIVE_INTERVAL_SEC", "45") or 45)
    except Exception:
        TELEGRAM_PREWARM_ENABLED = True
        TELEGRAM_PREWARM_DCS = [1, 2, 4, 5]
        TELEGRAM_KEEPALIVE_INTERVAL_SEC = 45

    try:
        # MKV/MP4 Tail & Cues RAM cache for instant seek index lookups on Android TV / ExoPlayer.
        TELEGRAM_TAIL_CACHE_ENABLED = getenv("TELEGRAM_TAIL_CACHE_ENABLED", "true").lower() == "true"
        TELEGRAM_TAIL_CACHE_SIZE_KB = int(getenv("TELEGRAM_TAIL_CACHE_SIZE_KB", "256") or 256)
        TELEGRAM_TAIL_CACHE_MAX_ENTRIES = int(getenv("TELEGRAM_TAIL_CACHE_MAX_ENTRIES", "64") or 64)
        # Slices initial header probe to 256 KB for ultra-fast codec negotiation.
        STREAM_RAMP_UP_CHUNK_KB = int(getenv("STREAM_RAMP_UP_CHUNK_KB", "256") or 256)
        # Saturates the multi-bot pool immediately on stream open to pre-fill ExoPlayer buffer in ~250ms.
        STREAM_BURST_PREFILL_ENABLED = getenv("STREAM_BURST_PREFILL_ENABLED", "true").lower() == "true"
        # Connection pressure & MTProto rate limit protection
        TELEGRAM_MAX_CONCURRENT_PER_CLIENT = int(getenv("TELEGRAM_MAX_CONCURRENT_PER_CLIENT", "6") or 6)
        TELEGRAM_MAX_GLOBAL_CONCURRENT_CHUNKS = int(getenv("TELEGRAM_MAX_GLOBAL_CONCURRENT_CHUNKS", "24") or 24)
        TELEGRAM_FLOODWAIT_AUTO_COOLDOWN = getenv("TELEGRAM_FLOODWAIT_AUTO_COOLDOWN", "true").lower() == "true"
        TELEGRAM_KEEPALIVE_JITTER_SEC = float(getenv("TELEGRAM_KEEPALIVE_JITTER_SEC", "3.0") or 3.0)
        # Stream Picker Pre-Buffering (0ms click-to-play)
        STREAM_PICKER_PREBUFFER_ENABLED = getenv("STREAM_PICKER_PREBUFFER_ENABLED", "true").lower() == "true"
        STREAM_PICKER_PREBUFFER_SIZE_KB = int(getenv("STREAM_PICKER_PREBUFFER_SIZE_KB", "256") or 256)
        STREAM_PICKER_PREBUFFER_MAX_ENTRIES = int(getenv("STREAM_PICKER_PREBUFFER_MAX_ENTRIES", "32") or 32)
    except Exception:
        TELEGRAM_TAIL_CACHE_ENABLED = True
        TELEGRAM_TAIL_CACHE_SIZE_KB = 256
        TELEGRAM_TAIL_CACHE_MAX_ENTRIES = 64
        STREAM_RAMP_UP_CHUNK_KB = 256
        STREAM_BURST_PREFILL_ENABLED = True
        TELEGRAM_MAX_CONCURRENT_PER_CLIENT = 6
        TELEGRAM_MAX_GLOBAL_CONCURRENT_CHUNKS = 24
        TELEGRAM_FLOODWAIT_AUTO_COOLDOWN = True
        TELEGRAM_KEEPALIVE_JITTER_SEC = 3.0
        STREAM_PICKER_PREBUFFER_ENABLED = True
        STREAM_PICKER_PREBUFFER_SIZE_KB = 256
        STREAM_PICKER_PREBUFFER_MAX_ENTRIES = 32

    try:
        # Container seek-index parsing (MKV Cues / MP4 moov) — maps keyframe
        # time <-> byte offset so skip pre-warm and runway math are exact.
        STREAM_INDEX_ENABLED = getenv("STREAM_INDEX_ENABLED", "true").lower() == "true"
        STREAM_INDEX_CACHE_MAX_ENTRIES = int(getenv("STREAM_INDEX_CACHE_MAX_ENTRIES", "64") or 64)
        STREAM_INDEX_MAX_KEYFRAMES = int(getenv("STREAM_INDEX_MAX_KEYFRAMES", "4096") or 4096)
        # Streamed-chunk spill cache — persist delivered chunks to a sparse disk
        # file so backward seeks / replays / second viewers never re-fetch MTProto.
        SPILL_CACHE_ENABLED = getenv("SPILL_CACHE_ENABLED", "true").lower() == "true"
        SPILL_CACHE_MAX_GB = float(getenv("SPILL_CACHE_MAX_GB", "2.0") or 2.0)
        # Runway-aware adaptive prefetch — boost PRE_FETCH toward PARALLEL when the
        # measured Telegram feed falls behind the file's bitrate, relax when ahead.
        RUNWAY_PREFETCH_ENABLED = getenv("RUNWAY_PREFETCH_ENABLED", "true").lower() == "true"
        RUNWAY_STARVE_RATIO = float(getenv("RUNWAY_STARVE_RATIO", "1.15") or 1.15)
        RUNWAY_RELAX_RATIO = float(getenv("RUNWAY_RELAX_RATIO", "2.0") or 2.0)
        RUNWAY_HEAD_BOOST_MAX_MB = float(getenv("RUNWAY_HEAD_BOOST_MAX_MB", "60") or 60)
        STREAM_BITRATE_HINT_ENABLED = getenv("STREAM_BITRATE_HINT_ENABLED", "false").lower() == "true"
        # Skip-target speculative pre-warm — fetch +10s/+30s/-10s 512KB windows
        # during playback so consecutive TV remote skips are RAM hits.
        SKIP_PREWARM_ENABLED = getenv("SKIP_PREWARM_ENABLED", "true").lower() == "true"
        SKIP_PREWARM_TARGETS_SEC = [
            float(x.strip())
            for x in (getenv("SKIP_PREWARM_TARGETS_SEC", "10,30,-10") or "10,30,-10").split(",")
            if x.strip()
        ]
        SKIP_PREWARM_MAX_INFLIGHT = int(getenv("SKIP_PREWARM_MAX_INFLIGHT", "2") or 2)
        # Multi-window seek cache + picker enhancements
        SEEK_CACHE_WINDOWS_PER_FILE = int(getenv("SEEK_CACHE_WINDOWS_PER_FILE", "4") or 4)
        STREAM_PICKER_PREBUFFER_CANDIDATES = int(getenv("STREAM_PICKER_PREBUFFER_CANDIDATES", "2") or 2)
        STREAM_PICKER_SESSION_PREWARM = getenv("STREAM_PICKER_SESSION_PREWARM", "true").lower() == "true"
    except Exception:
        STREAM_INDEX_ENABLED = True
        STREAM_INDEX_CACHE_MAX_ENTRIES = 64
        STREAM_INDEX_MAX_KEYFRAMES = 4096
        SPILL_CACHE_ENABLED = True
        SPILL_CACHE_MAX_GB = 2.0
        RUNWAY_PREFETCH_ENABLED = True
        RUNWAY_STARVE_RATIO = 1.15
        RUNWAY_RELAX_RATIO = 2.0
        RUNWAY_HEAD_BOOST_MAX_MB = 60
        STREAM_BITRATE_HINT_ENABLED = False
        SKIP_PREWARM_ENABLED = True
        SKIP_PREWARM_TARGETS_SEC = [10.0, 30.0, -10.0]
        SKIP_PREWARM_MAX_INFLIGHT = 2
        SEEK_CACHE_WINDOWS_PER_FILE = 4
        STREAM_PICKER_PREBUFFER_CANDIDATES = 2
        STREAM_PICKER_SESSION_PREWARM = True

    AUTH_CHANNEL = [channel.strip() for channel in (getenv("AUTH_CHANNEL") or "").split(",") if channel.strip()]
    MANUAL_CHANNELS = [channel.strip() for channel in (getenv("MANUAL_CHANNELS") or "").split(",") if channel.strip()]
    ANIME_CHANNELS = [channel.strip() for channel in (getenv("ANIME_CHANNELS") or "").split(",") if channel.strip()]
    GLOBAL_SEARCH = getenv("GLOBAL_SEARCH", "false").lower() == "true"
    GLOBAL_SEARCH_CHANNELS = [channel.strip() for channel in (getenv("GLOBAL_SEARCH_CHANNELS") or "").split(",") if channel.strip()]
    CONTENT_REQUESTS_ENABLED = getenv("CONTENT_REQUESTS_ENABLED", "false").lower() == "true"
    CONTENT_REQUESTS_BETA_ONLY = getenv("CONTENT_REQUESTS_BETA_ONLY", "true").lower() == "true"
    ANNOUNCE_NEW_CONTENT = getenv("ANNOUNCE_NEW_CONTENT", "false").lower() == "true"
    ANNOUNCEMENT_CHANNEL = getenv("ANNOUNCEMENT_CHANNEL", "").strip()
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
    NUVIO_WATCH_ENABLED = getenv("NUVIO_WATCH_ENABLED", "true").lower() == "true"

    PUBLIC_BETA_ENABLED = getenv("PUBLIC_BETA_ENABLED", "false").lower() == "true"
    BETA_INVITE_ONLY = getenv("BETA_INVITE_ONLY", "true").lower() == "true"
    BETA_ALLOWED_USER_IDS = [
        int(x.strip())
        for x in (getenv("BETA_ALLOWED_USER_IDS") or "").split(",")
        if x.strip().lstrip("-").isdigit()
    ]
    BETA_EXEMPT_USER_IDS = [
        int(x.strip())
        for x in (getenv("BETA_EXEMPT_USER_IDS") or "").split(",")
        if x.strip().lstrip("-").isdigit()
    ]
    BETA_EXEMPT_TOKEN_NAMES = [
        x.strip()
        for x in getenv("BETA_EXEMPT_TOKEN_NAMES", "autotest-temp").split(",")
        if x.strip()
    ]
    BETA_EXEMPT_TOKENS = [
        x.strip()
        for x in (getenv("BETA_EXEMPT_TOKENS") or "").split(",")
        if x.strip()
    ]
    BETA_WAITLIST_MESSAGE = getenv(
        "BETA_WAITLIST_MESSAGE",
        "This private beta is invite-only right now. Please contact the admin for access.",
    )
    REQUIRE_TERMS_ACCEPTANCE = getenv("REQUIRE_TERMS_ACCEPTANCE", "true").lower() == "true"
    TERMS_VERSION = getenv("TERMS_VERSION", "2026-07-07").strip()
    try:
        DEFAULT_TOKEN_DAILY_LIMIT_GB = float(getenv("DEFAULT_TOKEN_DAILY_LIMIT_GB", "25") or 25)
    except Exception:
        DEFAULT_TOKEN_DAILY_LIMIT_GB = 25.0
    try:
        DEFAULT_TOKEN_MONTHLY_LIMIT_GB = float(getenv("DEFAULT_TOKEN_MONTHLY_LIMIT_GB", "300") or 300)
    except Exception:
        DEFAULT_TOKEN_MONTHLY_LIMIT_GB = 300.0
    try:
        DEFAULT_TOKEN_MAX_ACTIVE_STREAMS = int(getenv("DEFAULT_TOKEN_MAX_ACTIVE_STREAMS", "2") or 2)
    except Exception:
        DEFAULT_TOKEN_MAX_ACTIVE_STREAMS = 2
    try:
        MAX_ACTIVE_STREAMS_GLOBAL = int(getenv("MAX_ACTIVE_STREAMS_GLOBAL", "4") or 4)
    except Exception:
        MAX_ACTIVE_STREAMS_GLOBAL = 4
    try:
        STREAM_LOG_RETENTION_DAYS = int(getenv("STREAM_LOG_RETENTION_DAYS", "30") or 30)
    except Exception:
        STREAM_LOG_RETENTION_DAYS = 30
    try:
        BILLING_LOG_RETENTION_DAYS = int(getenv("BILLING_LOG_RETENTION_DAYS", "180") or 180)
    except Exception:
        BILLING_LOG_RETENTION_DAYS = 180
    OWNER_ALERTS_ENABLED = getenv("OWNER_ALERTS_ENABLED", "true").lower() == "true"
    BACKUP_ENABLED = getenv("BACKUP_ENABLED", "true").lower() == "true"
    BACKUP_DIR = getenv("BACKUP_DIR", "backups/production").strip() or "backups/production"
    try:
        BACKUP_INTERVAL_HOURS = max(1, int(getenv("BACKUP_INTERVAL_HOURS", "24") or 24))
    except Exception:
        BACKUP_INTERVAL_HOURS = 24
    
    SUBSCRIPTION = getenv("SUBSCRIPTION", "false").lower() == "true"
    SUBSCRIPTION_GROUP_ID = int(getenv("SUBSCRIPTION_GROUP_ID", "0"))
    SUBSCRIPTION_URL = getenv("SUBSCRIPTION_URL", "https://t.me/")
    APPROVER_IDS = [int(x.strip()) for x in (getenv("APPROVER_IDS") or "").split(",") if x.strip().isdigit()]

    PROXY = getenv("Proxy", "false").lower() == "true"
    PROXY_TYPE = getenv("ProxyType", "HTTPS")
    HTTP_PROXY_URL = getenv("HTTP_Proxy_URL", "")
    SHOW_PROXY_AND_NON_PROXY_BOTH = getenv("SHOW_ProxyAndNonProxyBoth", "false").lower() == "true"

    # -------------------------------
    # Disk cache + nginx offload (Tier 2 fast-start buffer)
    # -------------------------------
    DISK_CACHE_ENABLED = getenv("DISK_CACHE_ENABLED", "true").lower() == "true"
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
    # Fast-start prefix cache: cache the first N MiB of a file so
    # the opening of streams is served from local NVMe disk while the rest continues from Telegram.
    try:
        DISK_CACHE_FIRST_MB = float(getenv("DISK_CACHE_FIRST_MB", "0") or 0)
    except Exception:
        DISK_CACHE_FIRST_MB = 0.0

    # ExoPlayer Seek & Scrub Micro-Range Coalescing
    SEEK_COALESCING_ENABLED = getenv("SEEK_COALESCING_ENABLED", "true").lower() == "true"
    SEEK_CACHE_MAX_ENTRIES = int(getenv("SEEK_CACHE_MAX_ENTRIES", "16") or 16)
    SEEK_CACHE_TTL_SEC = float(getenv("SEEK_CACHE_TTL_SEC", "10.0") or 10.0)

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
