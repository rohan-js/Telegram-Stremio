from fastapi import HTTPException, Request
from fastapi.security import HTTPBearer
from typing import Optional
from Backend.helper.passwords import verify_password
from Backend.helper.settings_manager import SettingsManager

security = HTTPBearer(auto_error=False)

def verify_credentials(username: str, password: str) -> bool:
    settings = SettingsManager.current()
    return username == settings.admin_username and verify_password(password, settings.admin_password)

def is_authenticated(request: Request) -> bool:
    return request.session.get("authenticated", False)

def require_auth(request: Request):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Authentication required")
    return True

def get_current_user(request: Request) -> Optional[str]:
    if is_authenticated(request):
        return request.session.get("username")
    return None
