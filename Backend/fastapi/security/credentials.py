from fastapi import HTTPException, Request
from fastapi.security import HTTPBearer
from typing import Optional
from Backend.helper.bounded import BoundedDict
from Backend.helper.passwords import verify_password
from Backend.helper.settings_manager import SettingsManager

security = HTTPBearer(auto_error=False)

#----- Session ids revoked via logout (upstream 1de5e72): the signed cookie can be
#----- re-presented after session.clear(), so the sid must be checked server-side.
_revoked_sids = BoundedDict(1000)

def verify_credentials(username: str, password: str) -> bool:
    settings = SettingsManager.current()
    return username == settings.admin_username and verify_password(password, settings.admin_password)

def is_authenticated(request: Request) -> bool:
    if not request.session.get("authenticated", False):
        return False
    if request.session.get("sid") in _revoked_sids:
        request.session.clear()
        return False
    return True

def revoke_session(request: Request) -> None:
    sid = request.session.get("sid")
    if sid:
        _revoked_sids[sid] = True
    request.session.clear()

def require_auth(request: Request):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Authentication required")
    return True

def get_current_user(request: Request) -> Optional[str]:
    if is_authenticated(request):
        return request.session.get("username")
    return None
