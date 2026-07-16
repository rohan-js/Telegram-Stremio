WATCH_CALLBACK_PREFIX = "watch_"
NUVIO_CALLBACK_PREFIX = "nuvio_"
MAX_CALLBACK_DATA_BYTES = 64


def watch_callback_data(request_id: str) -> str:
    return f"{WATCH_CALLBACK_PREFIX}{request_id}"


def nuvio_callback_data(request_id: str) -> str:
    return f"{NUVIO_CALLBACK_PREFIX}{request_id}"


def callback_data_fits(callback_data: str) -> bool:
    return len(str(callback_data or "").encode("utf-8")) <= MAX_CALLBACK_DATA_BYTES


def telegram_user_display_name(
    first_name: str | None = None,
    last_name: str | None = None,
    username: str | None = None,
    user_id: int | str | None = None,
) -> str:
    name = " ".join(part for part in [first_name, last_name] if part).strip()
    if name:
        return name
    if username:
        return username
    if user_id:
        return f"User {user_id}"
    return "Telegram User"
