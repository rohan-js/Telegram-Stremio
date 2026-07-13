import sys

import uvicorn
from Backend.config import Telegram
from Backend.fastapi.main import app


Port = Telegram.PORT


def _server_implementations() -> tuple[str, str]:
    if sys.platform == "win32":
        return "asyncio", "h11"

    loop_impl, http_impl = "asyncio", "h11"
    try:
        import uvloop  # noqa: F401
        loop_impl = "uvloop"
    except ImportError:
        pass
    try:
        import httptools  # noqa: F401
        http_impl = "httptools"
    except ImportError:
        pass
    return loop_impl, http_impl


loop_impl, http_impl = _server_implementations()
config = uvicorn.Config(app=app, host='0.0.0.0', port=Port, loop=loop_impl, http=http_impl)
server = uvicorn.Server(config)
