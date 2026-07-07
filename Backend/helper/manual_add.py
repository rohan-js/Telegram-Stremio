import re
from typing import Optional, Tuple

import PTN
from Backend.helper.pyro import clean_filename, get_readable_file_size, is_media
from Backend.helper.split_files import parse_split_info, strip_part_suffix

_PRIVATE_LINK = re.compile(r"t\.me/c/(\d+)(?:/\d+)*/(\d+)")
_PUBLIC_LINK = re.compile(r"t\.me/([A-Za-z][\w]{3,})/(?:\d+/)?(\d+)")


def parse_telegram_link(url: str) -> Tuple[Optional[object], Optional[int]]:
    url = (url or "").strip()
    private = _PRIVATE_LINK.search(url)
    if private:
        return int(f"-100{private.group(1)}"), int(private.group(2))
    public = _PUBLIC_LINK.search(url)
    if public:
        return public.group(1), int(public.group(2))
    return None, None


def quality_from_height(height: int) -> str:
    if not height:
        return ""
    for threshold, label in (
        (1800, "2160p"),
        (1200, "1440p"),
        (900, "1080p"),
        (620, "720p"),
        (400, "480p"),
        (260, "360p"),
    ):
        if height >= threshold:
            return label
    return "240p"


def finalize_manual_name(raw_name: str, is_split: bool = False) -> str:
    cleaned = clean_filename(raw_name or "video")
    if is_split:
        cleaned = strip_part_suffix(cleaned)
    return cleaned


async def resolve_telegram_message(client, url: str = None, chat_id=None, msg_id=None) -> dict:
    if url:
        chat_ref, msg_id = parse_telegram_link(url)
        if chat_ref is None:
            raise ValueError("Could not read that Telegram link. Use a t.me/c/... or t.me/<channel>/... message link.")
    elif chat_id and msg_id:
        chat_ref = int(f"-100{str(chat_id).replace('-100', '')}")
        msg_id = int(msg_id)
    else:
        raise ValueError("Provide a Telegram message link, or a chat id and message id.")

    message = await client.get_messages(chat_ref, msg_id)
    if not message or getattr(message, "empty", False):
        raise ValueError("That message was not found. Make sure the bot can access the channel.")

    media = is_media(message)
    if not media:
        raise ValueError("That message has no downloadable file.")

    caption = (getattr(message, "caption", None) or "").strip()
    raw_name = caption or getattr(media, "file_name", None) or "video"
    cleaned = clean_filename(raw_name)
    split_info = parse_split_info(cleaned)
    try:
        parsed = PTN.parse(strip_part_suffix(cleaned) if split_info else cleaned)
    except Exception:
        parsed = {}
    raw_size = getattr(media, "file_size", 0) or 0
    height = getattr(media, "height", 0) or 0
    original_date = getattr(message, "forward_date", None) or getattr(message, "date", None)

    return {
        "chat_id": str(message.chat.id).replace("-100", ""),
        "msg_id": message.id,
        "name": finalize_manual_name(raw_name, bool(split_info)),
        "raw_size": raw_size,
        "size": get_readable_file_size(raw_size),
        "quality": quality_from_height(height) or parsed.get("resolution") or parsed.get("quality") or "",
        "season": parsed.get("season"),
        "episode": parsed.get("episode"),
        "width": getattr(media, "width", 0) or 0,
        "height": height,
        "has_thumb": bool(getattr(media, "thumbs", None)),
        "upload_year": original_date.year if original_date else 0,
    }
