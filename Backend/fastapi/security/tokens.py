from fastapi import HTTPException
from Backend import db
from Backend.logger import LOGGER

LIMIT_EXCEEDED_VIDEO = "https://bit.ly/3YZFKT5"


async def verify_token(token: str) -> dict:
    """
    Verify token and check daily limit.
    Returns token data if valid, raises HTTPException if invalid.
    """
    token_data = await db.get_token(token)
    
    if not token_data:
        raise HTTPException(
            status_code=401, 
            detail="Invalid or expired token. Get a subscription at /start"
        )
    
    # Check daily limit
    within_limit, remaining = await db.check_daily_limit(token)
    
    if not within_limit:
        token_data["limit_exceeded"] = True
        token_data["limit_video"] = LIMIT_EXCEEDED_VIDEO
        LOGGER.warning(f"Daily limit exceeded for token {token[:8]}...")
    else:
        token_data["limit_exceeded"] = False
        token_data["remaining_gb"] = remaining
    
    return token_data


async def get_optional_token(token: str = None) -> dict:
    """
    Optional token verification - returns None if no token provided.
    Used for endpoints that work with or without authentication.
    """
    if not token:
        return None
    
    try:
        return await verify_token(token)
    except HTTPException:
        return None
