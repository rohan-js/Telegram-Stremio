from fastapi import HTTPException
from datetime import datetime
from Backend import db
from Backend.config import Telegram
from Backend.helper.beta_access import is_exempt_token
from Backend.helper.owner_alerts import schedule_owner_alert

DAILY_LIMIT_VIDEO = "https://bit.ly/3YZFKT5"
MONTHLY_LIMIT_VIDEO = "https://bit.ly/4rfjtgd"
SUBSCRIPTION_EXPIRED_VIDEO = "https://bit.ly/4rfjtgd"
ACTIVE_STREAM_LIMIT_VIDEO = "https://bit.ly/4rfjtgd"


def _active_stream_counts(token: str) -> tuple[int, int]:
    try:
        from Backend.helper.custom_dl import ACTIVE_STREAMS
    except Exception:
        return 0, 0
    token_active = 0
    global_active = 0
    for info in ACTIVE_STREAMS.values():
        if info.get("status", "active") != "active":
            continue
        global_active += 1
        meta = info.get("meta") or {}
        if meta.get("token") == token:
            token_active += 1
    return token_active, global_active


def enforce_playback_token(token_data: dict):
    if not token_data:
        raise HTTPException(status_code=401, detail="Invalid token")
    if token_data.get("subscription_expired"):
        raise HTTPException(status_code=403, detail="Subscription expired")
    if token_data.get("limit_exceeded"):
        raise HTTPException(status_code=429, detail=f"Streaming limit reached: {token_data.get('limit_exceeded')}")


async def verify_token(token: str):
    token_data = await db.get_api_token(token)
    if not token_data:
        raise HTTPException(status_code=401, detail="Invalid or expired API token")

    limits = token_data.get("limits", {})
    usage = token_data.get("usage", {})

    token_data["limit_exceeded"] = None
    token_data["limit_video"] = None
    token_data["subscription_expired"] = False
    token_data["is_beta_exempt"] = is_exempt_token(token_data)

    if token_data["is_beta_exempt"]:
        return token_data

    # --- Subscription expiry check (only when SUBSCRIPTION feature is enabled) ---
    if Telegram.SUBSCRIPTION:
        user_id = token_data.get("user_id")
        if not user_id:
            # Token has no linked user — treat as expired (unverified token)
            token_data["subscription_expired"] = True
            return token_data

        user = await db.get_user(int(user_id))
        if not user or user.get("subscription_status") != "active":
            token_data["subscription_expired"] = True
            return token_data

        expiry = user.get("subscription_expiry")
        if not expiry:
            token_data["subscription_expired"] = True
            return token_data

        # Compare correctly regardless of timezone awareness
        now = datetime.utcnow()
        try:
            if expiry.tzinfo is not None:
                from datetime import timezone
                now = datetime.now(timezone.utc)
        except AttributeError:
            pass
        if expiry < now:
            token_data["subscription_expired"] = True
            return token_data

    if daily_limit := limits.get("daily_limit_gb"):
        if daily_limit > 0:
            current_daily_gb = usage.get("daily", {}).get("bytes", 0) / (1024 ** 3)
            if current_daily_gb >= daily_limit:
                token_data["limit_exceeded"] = "daily"
                token_data["limit_video"] = DAILY_LIMIT_VIDEO
                return token_data

    if monthly_limit := limits.get("monthly_limit_gb"):
        if monthly_limit > 0:
            current_monthly_gb = usage.get("monthly", {}).get("bytes", 0) / (1024 ** 3)
            if current_monthly_gb >= monthly_limit:
                token_data["limit_exceeded"] = "monthly"
                token_data["limit_video"] = MONTHLY_LIMIT_VIDEO
                return token_data

    token_active, global_active = _active_stream_counts(token)
    token_data["active_streams_current"] = token_active
    token_data["active_streams_global"] = global_active
    token_limit = int(limits.get("max_active_streams") or getattr(Telegram, "DEFAULT_TOKEN_MAX_ACTIVE_STREAMS", 2) or 2)
    global_limit = int(getattr(Telegram, "MAX_ACTIVE_STREAMS_GLOBAL", 4) or 4)
    if token_limit > 0 and token_active >= token_limit:
        token_data["limit_exceeded"] = "active_streams"
        token_data["limit_video"] = ACTIVE_STREAM_LIMIT_VIDEO
        schedule_owner_alert(
            f"Token active stream limit reached for {token_data.get('name') or token[:8]} ({token_active}/{token_limit}).",
            key=f"token-active-limit:{token}",
            cooldown_sec=600,
        )
        return token_data
    if global_limit > 0 and global_active >= global_limit:
        token_data["limit_exceeded"] = "global_active_streams"
        token_data["limit_video"] = ACTIVE_STREAM_LIMIT_VIDEO
        schedule_owner_alert(
            f"Global active stream limit reached ({global_active}/{global_limit}).",
            key="global-active-limit",
            cooldown_sec=600,
        )
        return token_data

    return token_data
