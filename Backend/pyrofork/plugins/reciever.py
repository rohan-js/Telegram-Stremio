from asyncio import Queue, Lock, create_task, sleep as asleep

import Backend
from pyrogram import Client, filters
from pyrogram.enums.parse_mode import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from Backend import db
from Backend.config import Telegram
from Backend.helper.encrypt import encode_string
from Backend.helper.metadata import metadata, extract_default_id
from Backend.helper.pyro import clean_filename, get_readable_file_size, remove_urls
from Backend.helper.task_manager import edit_message
from Backend.logger import LOGGER
from Backend.helper.disk_cache import PRECACHE_MANAGER, PrecacheJob


file_queue = Queue()
db_lock = Lock()
reply_queue = Queue()


async def process_file():
    while True:
        metadata_info, channel, msg_id, size, title, chat_id, original_msg_id = await file_queue.get()
        async with db_lock:
            updated_id = await db.insert_media(
                metadata_info,
                channel=channel,
                msg_id=msg_id,
                size=size,
                name=title,
            )
            if updated_id:
                LOGGER.info(f"{metadata_info['media_type']} updated with ID: {updated_id}")
                await reply_queue.put((chat_id, original_msg_id, metadata_info, title, size))
            else:
                LOGGER.info("Update failed due to validation errors.")
        file_queue.task_done()


async def send_reply_messages():
    from Backend.pyrofork.bot import StreamBot

    while True:
        chat_id, msg_id, metadata_info, title, size = await reply_queue.get()
        try:
            base_url = Telegram.BASE_URL.rstrip("/")
            token = Telegram.DEFAULT_ADDON_TOKEN
            imdb_id = metadata_info.get("imdb_id", "")
            media_type = metadata_info.get("media_type", "movie")
            movie_title = metadata_info.get("title", "Unknown")
            year = metadata_info.get("year", "")
            quality = metadata_info.get("quality", "")
            encoded_string = metadata_info.get("encoded_string", "")
            rating = metadata_info.get("rate", "")

            if imdb_id:
                if media_type == "tv":
                    season = metadata_info.get("season_number", 1)
                    episode = metadata_info.get("episode_number", 1)
                    stremio_link = f"{base_url}/stremio/open/series/{imdb_id}?season={season}&episode={episode}"
                else:
                    stremio_link = f"{base_url}/stremio/open/movie/{imdb_id}"
            else:
                stremio_link = f"{base_url}/stremio/{token}/configure" if token else f"{base_url}/stremio"

            if encoded_string and token:
                direct_stream = f"{base_url}/dl/{token}/{encoded_string}/video.mkv"
                vlc_page = f"{base_url}/vlc/{token}/{encoded_string}"
            else:
                direct_stream = "N/A"
                vlc_page = None

            rating_str = f"⭐ {rating}" if rating else ""
            help_note = (
                "⚠️ If streaming is slow or not loading, turn on Cloudflare WARP and try again.\n"
                "Facing any issues? Type it here itself.\n\n"
            )
            if media_type == "tv":
                season = int(metadata_info.get("season_number", 0) or 0)
                episode = int(metadata_info.get("episode_number", 0) or 0)
                ep_title = metadata_info.get("episode_title", "")
                if metadata_info.get("season_pack"):
                    episode_count = int(metadata_info.get("season_pack_episode_count", 0) or 0)
                    reply_text = (
                        f"🎬 <b>{movie_title}</b>"
                        f"{f' ({year})' if year else ''}\n"
                        f"📺 Season {season:02d} Pack"
                        f"{f' | {episode_count} episodes' if episode_count else ''}\n"
                        f"{rating_str}"
                        f"{f' | {quality}' if quality else ''}"
                        f" | {size}\n\n"
                        f"{help_note}"
                        f"▶️ <b>Direct Stream Link:</b>\n<code>{direct_stream}</code>"
                    )
                else:
                    reply_text = (
                        f"🎬 <b>{movie_title}</b>"
                        f"{f' ({year})' if year else ''}\n"
                        f"📺 S{season:02d}E{episode:02d}"
                        f"{f' - {ep_title}' if ep_title else ''}\n"
                        f"{rating_str}"
                        f"{f' | {quality}' if quality else ''}"
                        f" | {size}\n\n"
                        f"{help_note}"
                        f"▶️ <b>Direct Stream Link:</b>\n<code>{direct_stream}</code>"
                    )
            else:
                reply_text = (
                    f"🎬 <b>{movie_title}</b>"
                    f"{f' ({year})' if year else ''}\n"
                    f"{rating_str}"
                    f"{f' | {quality}' if quality else ''}"
                    f" | {size}\n\n"
                    f"{help_note}"
                    f"▶️ <b>Direct Stream Link:</b>\n<code>{direct_stream}</code>"
                )

            buttons_row = [InlineKeyboardButton("▶️ Watch in Stremio", url=stremio_link)]
            if vlc_page:
                buttons_row.append(InlineKeyboardButton("🎬 Watch in VLC", url=vlc_page))
            buttons = InlineKeyboardMarkup([buttons_row])

            await StreamBot.send_message(
                chat_id=chat_id,
                text=reply_text,
                reply_to_message_id=msg_id,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=buttons,
            )
            LOGGER.info(f"Sent stream link reply for: {movie_title}")
        except FloodWait as e:
            LOGGER.info(f"FloodWait in reply: sleeping for {e.value}s")
            await asleep(e.value)
        except Exception as e:
            LOGGER.error(f"Failed to send reply message: {e}")
        reply_queue.task_done()


create_task(process_file())
create_task(send_reply_messages())


@Client.on_message(filters.channel & (filters.document | filters.video))
async def file_receive_handler(client: Client, message: Message):
    if str(message.chat.id) in Telegram.AUTH_CHANNEL:
        try:
            video_extensions = (".mkv", ".mp4", ".avi", ".webm", ".mov", ".flv", ".wmv")
            is_video_doc = (
                message.document and (
                    (message.document.mime_type and message.document.mime_type.startswith("video/")) or
                    (message.document.file_name and message.document.file_name.lower().endswith(video_extensions))
                )
            )
            if message.video or is_video_doc:
                file = message.video or message.document
                file_name = getattr(file, "file_name", None) or ""
                if file_name and file_name.lower().endswith(video_extensions):
                    title = file_name
                else:
                    title = message.caption or file_name or "unknown"

                msg_id = message.id
                size = get_readable_file_size(file.file_size)
                channel = str(message.chat.id).replace("-100", "")

                # Background pre-cache to disk (optional, behind config flags)
                try:
                    if getattr(file, "file_unique_id", None) and getattr(file, "file_size", None):
                        create_task(
                            PRECACHE_MANAGER.enqueue(
                                client,
                                PrecacheJob(
                                    chat_id=int(message.chat.id),
                                    msg_id=int(msg_id),
                                    unique_id=str(file.file_unique_id),
                                    expected_size=int(file.file_size or 0),
                                ),
                            )
                        )
                except Exception as e:
                    LOGGER.debug(f"Precache enqueue failed for msg {msg_id}: {e}")

                metadata_info = await metadata(clean_filename(title), int(channel), msg_id)
                if metadata_info is None:
                    LOGGER.warning(f"Metadata failed for file: {title} (ID: {msg_id})")
                    await message.reply_text(
                        "Metadata failed for this file. If it is a TV file, use an episode filename like S01E01, "
                        "or use S01 COMBINED for a full-season pack."
                    )
                    return

                title = remove_urls(title)
                if not title.endswith((".mkv", ".mp4")):
                    title += ".mkv"

                if Backend.USE_DEFAULT_ID:
                    new_caption = (message.caption + "\n\n" + Backend.USE_DEFAULT_ID) if message.caption else Backend.USE_DEFAULT_ID
                    create_task(edit_message(chat_id=message.chat.id, msg_id=message.id, new_caption=new_caption))

                await file_queue.put((metadata_info, int(channel), msg_id, size, title, message.chat.id, message.id))
            else:
                await message.reply_text("> Not supported")
        except FloodWait as e:
            LOGGER.info(f"Sleeping for {str(e.value)}s")
            await asleep(e.value)
            await message.reply_text(
                text=f"Got Floodwait of {str(e.value)}s",
                disable_web_page_preview=True,
                parse_mode=ParseMode.MARKDOWN,
            )
    else:
        await message.reply_text("> Channel is not in AUTH_CHANNEL")


@Client.on_edited_message(filters.channel & (filters.document | filters.video))
async def file_edited_handler(client: Client, message: Message):
    if str(message.chat.id) in Telegram.AUTH_CHANNEL:
        try:
            if message.video or (message.document and message.document.mime_type.startswith("video/")):
                file = message.video or message.document
                title = message.caption or file.file_name
                msg_id = message.id
                size = get_readable_file_size(file.file_size)
                channel = str(message.chat.id).replace("-100", "")
                override_id = extract_default_id(message.caption) if message.caption else None

                # Pre-cache edited media too (in case the file itself changed)
                try:
                    if getattr(file, "file_unique_id", None) and getattr(file, "file_size", None):
                        create_task(
                            PRECACHE_MANAGER.enqueue(
                                client,
                                PrecacheJob(
                                    chat_id=int(message.chat.id),
                                    msg_id=int(msg_id),
                                    unique_id=str(file.file_unique_id),
                                    expected_size=int(file.file_size or 0),
                                ),
                            )
                        )
                except Exception as e:
                    LOGGER.debug(f"Precache enqueue failed for edited msg {msg_id}: {e}")

                if override_id:
                    LOGGER.info(f"Detected override ID '{override_id}' in edited message {msg_id}")
                    stream_id_hash = await encode_string({"chat_id": int(channel), "msg_id": msg_id})
                    await db.delete_media_by_stream_id(stream_id_hash)
                    metadata_info = await metadata(clean_filename(title), int(channel), msg_id, override_id=override_id)
                    if metadata_info is None:
                        LOGGER.warning(f"Metadata failed for edited file: {title} (ID: {msg_id})")
                        await message.reply_text(
                            "Metadata failed for this edited file. If it is a TV file, use an episode filename like S01E01, "
                            "or use S01 COMBINED for a full-season pack."
                        )
                        return
                    title = remove_urls(title)
                    if not title.endswith((".mkv", ".mp4")):
                        title += ".mkv"
                    await file_queue.put((metadata_info, int(channel), msg_id, size, title, message.chat.id, message.id))
        except Exception as e:
            LOGGER.error(f"Error handling edited generic file {message.id}: {e}")


@Client.on_deleted_messages(filters.channel)
async def file_deleted_handler(client: Client, messages: list[Message]):
    try:
        for message in messages:
            if message.chat and str(message.chat.id) in Telegram.AUTH_CHANNEL:
                channel = str(message.chat.id).replace("-100", "")
                msg_id = message.id
                try:
                    stream_id_hash = await encode_string({"chat_id": int(channel), "msg_id": msg_id})
                    deleted = await db.delete_media_by_stream_id(stream_id_hash)
                    if deleted:
                        LOGGER.info(f"Automatically purged deleted message {msg_id} from database.")
                except Exception as ex:
                    LOGGER.error(f"Failed to scrub deleted message {msg_id}: {ex}")
    except Exception as e:
        LOGGER.error(f"Error handling deleted messages: {e}")
