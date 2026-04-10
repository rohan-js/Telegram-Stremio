import uvicorn
from Backend.config import Telegram
from Backend.fastapi.main import app


Port = Telegram.PORT
config = uvicorn.Config(
	app=app,
	host='0.0.0.0',
	port=Port,
	timeout_keep_alive=Telegram.UVICORN_KEEPALIVE,
	backlog=Telegram.UVICORN_BACKLOG,
	limit_concurrency=Telegram.UVICORN_CONCURRENCY,
	access_log=False,
)
server = uvicorn.Server(config)
