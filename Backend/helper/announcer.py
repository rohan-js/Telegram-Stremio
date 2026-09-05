from asyncio import create_task
from datetime import datetime

from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from Backend import db
from Backend.config import Telegram
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER
from Backend.pyrofork.bot import StreamBot


def _resolve_chat(value: str):
    value = str(value or "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return value


async def _claim(media_type: str, tmdb_id) -> bool:
    if not tmdb_id:
        return False
    result = await db.dbs["tracking"]["announced"].update_one(
        {"_id": f"{media_type}:{tmdb_id}"},
        {"$setOnInsert": {"at": datetime.utcnow()}},
        upsert=True,
    )
    return result.upserted_id is not None


def _caption(info: dict) -> str:
    is_tv = info.get("media_type") == "tv"
    title = info.get("title") or "Unknown"
    header = f"{'📺' if is_tv else '🎬'} <b>{title}</b>"
    if info.get("year"):
        header += f" ({info['year']})"
    lines = [header, "", f"🗂 <b>Type:</b> {'Series' if is_tv else 'Movie'}"]
    if info.get("rate"):
        lines.append(f"⭐ <b>Rating:</b> {info.get('rate')}")
    if info.get("genres"):
        lines.append(f"🎭 <b>Genres:</b> {', '.join((info.get('genres') or [])[:4])}")
    if info.get("quality"):
        lines.append(f"📶 <b>Quality:</b> {info.get('quality')}")
    desc = (info.get("description") or "").strip()
    if desc:
        lines += ["", f"<i>{desc[:317].rstrip() + '...' if len(desc) > 320 else desc}</i>"]
    return "\n".join(lines)


async def _announce(info: dict) -> None:
    settings = SettingsManager.current()
    if not getattr(settings, "announce_new_content", False):
        return
    chat = _resolve_chat(getattr(settings, "announcement_channel", ""))
    if chat is None:
        return
    if not await _claim(info.get("media_type"), info.get("tmdb_id")):
        return
    markup = None
    if Telegram.BASE_URL and Telegram.DEFAULT_ADDON_TOKEN:
        base = Telegram.BASE_URL.rstrip("/")
        rows = []
        imdb_id = str(info.get("imdb_id") or "").strip()
        stremio_type = "series" if info.get("media_type") == "tv" else "movie"
        if imdb_id:
            rows.append([
                InlineKeyboardButton("▶️ Stremio", url=f"{base}/stremio/open/{stremio_type}/{imdb_id}"),
                InlineKeyboardButton("📱 Nuvio", url=f"{base}/nuvio/open/{stremio_type}/{imdb_id}"),
            ])
        rows.append([
            InlineKeyboardButton(
                "🤖 Open Addon",
                url=f"{base}/stremio/{Telegram.DEFAULT_ADDON_TOKEN}/configure",
            )
        ])
        markup = InlineKeyboardMarkup(rows)
    poster = info.get("backdrop") or info.get("poster")
    try:
        sent = None
        if poster:
            try:
                sent = await StreamBot.send_photo(chat, poster, caption=_caption(info), parse_mode=ParseMode.HTML, reply_markup=markup)
            except FloodWait:
                raise
            except Exception:
                sent = None
        if sent is None:
            sent = await StreamBot.send_message(chat, _caption(info), parse_mode=ParseMode.HTML, reply_markup=markup, disable_web_page_preview=True)
        # remember where the announcement lives so media removal can delete it
        try:
            await db.dbs["tracking"]["announced"].update_one(
                {"_id": f"{info.get('media_type')}:{info.get('tmdb_id')}"},
                {"$set": {"chat_id": chat, "message_id": sent.id}},
            )
        except Exception:
            pass
    except FloodWait as exc:
        LOGGER.warning("Announcement FloodWait for %ss", exc.value)
    except Exception as exc:
        LOGGER.error("Announcement failed for %s: %s", info.get("title"), exc)


def announce_new_media(info: dict) -> None:
    if (info or {}).get("source_type") == "subtitle":
        return
    try:
        create_task(_announce(dict(info or {})))
    except RuntimeError:
        LOGGER.debug("Announcement skipped: no running event loop")


#----- Delete the announcement post when the media leaves the library
async def delete_announcement(media_type: str, tmdb_id) -> None:
    if not tmdb_id:
        return
    key = f"{media_type}:{tmdb_id}"
    try:
        doc = await db.dbs["tracking"]["announced"].find_one_and_delete({"_id": key})
    except Exception as e:
        LOGGER.warning(f"Failed to look up announcement for {key}: {e}")
        return
    if not doc:
        return
    chat_id = doc.get("chat_id")
    message_id = doc.get("message_id")
    if not chat_id or not message_id:
        return
    try:
        await StreamBot.delete_messages(chat_id, message_id)
        LOGGER.info(f"Deleted announcement message {message_id} for {key}")
    except FloodWait as e:
        LOGGER.warning(f"FloodWait deleting announcement for {key}: {e.value}s")
    except Exception as e:
        LOGGER.warning(f"Could not delete announcement for {key}: {e}")


def delete_announcement_async(media_type: str, tmdb_id) -> None:
    try:
        create_task(delete_announcement(media_type, tmdb_id))
    except RuntimeError:
        LOGGER.debug("Announcement delete skipped: no running event loop")
