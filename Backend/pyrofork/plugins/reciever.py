from asyncio import create_task, sleep as asleep, Queue, Lock
import Backend
from Backend.helper.task_manager import edit_message
from Backend.logger import LOGGER
from Backend import db
from Backend.config import Telegram
from Backend.helper.pyro import clean_filename, get_readable_file_size, remove_urls
from Backend.helper.metadata import metadata
from Backend.pyrofork.bot import StreamBot
from pyrogram import filters, Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait
from pyrogram.enums.parse_mode import ParseMode
import re


file_queue = Queue()
db_lock = Lock()
reply_queue = Queue()


async def process_file():
    while True:
        metadata_info, channel, msg_id, size, title, source_chat_id, source_msg_id = await file_queue.get()
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
                await reply_queue.put((source_chat_id, source_msg_id, metadata_info, size))
            else:
                LOGGER.info("Update failed due to validation errors.")
                try:
                    await StreamBot.send_message(
                        chat_id=source_chat_id,
                        text="⚠️ Metadata update failed. Check filename/caption format.",
                        reply_to_message_id=source_msg_id,
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except Exception as reply_error:
                    LOGGER.debug(f"Unable to send index failure reply: {reply_error}")
        file_queue.task_done()


async def send_reply_messages():
    while True:
        source_chat_id, source_msg_id, metadata_info, size = await reply_queue.get()
        try:
            base_url = Telegram.BASE_URL.rstrip("/")
            imdb_id_raw = (metadata_info.get("imdb_id", "") or "").strip()
            imdb_match = re.search(r"tt\d+", imdb_id_raw.lstrip("#"))
            imdb_id = imdb_match.group(0) if imdb_match else imdb_id_raw.lstrip("#")
            media_type = metadata_info.get("media_type", "movie")
            movie_title = metadata_info.get("title", "Unknown")
            year = metadata_info.get("year", "")
            quality = metadata_info.get("quality", "")
            encoded_string = metadata_info.get("encoded_string", "")
            rating = metadata_info.get("rate", "")

            # Keep legacy direct-link style (without token) as requested.
            direct_stream = f"{base_url}/dl/{encoded_string}/video.mkv" if encoded_string else base_url

            if imdb_id:
                if media_type == "tv":
                    season = metadata_info.get("season_number", 1)
                    episode = metadata_info.get("episode_number", 1)
                    stremio_link = f"{base_url}/stremio/open/series/{imdb_id}?season={season}&episode={episode}"
                else:
                    stremio_link = f"{base_url}/stremio/open/movie/{imdb_id}"
            else:
                stremio_link = f"{base_url}/stremio"

            vlc_page = f"{base_url}/vlc/{encoded_string}" if encoded_string else base_url

            rating_str = f"⭐ {rating}" if rating else ""
            quality_str = quality if quality else "Unknown"

            if media_type == "tv":
                season = metadata_info.get("season_number", "")
                episode = metadata_info.get("episode_number", "")
                ep_title = metadata_info.get("episode_title", "")
                reply_text = (
                    f"🎬 **{movie_title}**"
                    f"{f' ({year})' if year else ''}\n"
                    f"📺 S{int(season):02d}E{int(episode):02d}"
                    f"{f' - {ep_title}' if ep_title else ''}\n"
                    f"{rating_str} | {quality_str} | {size}\n\n"
                    f"▶️ **Direct Stream Link:**\n{direct_stream}"
                )
            else:
                reply_text = (
                    f"🎬 **{movie_title}**"
                    f"{f' ({year})' if year else ''}\n"
                    f"{rating_str} | {quality_str} | {size}\n\n"
                    f"▶️ **Direct Stream Link:**\n{direct_stream}"
                )

            buttons = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("▶️ Watch in Stremio", url=stremio_link),
                    InlineKeyboardButton("🎬 Watch in VLC", url=vlc_page),
                ]
            ])

            await StreamBot.send_message(
                chat_id=source_chat_id,
                text=reply_text,
                reply_to_message_id=source_msg_id,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
                reply_markup=buttons,
            )
        except FloodWait as e:
            LOGGER.info(f"FloodWait in reply: sleeping for {e.value}s")
            await asleep(e.value)
        except Exception as e:
            LOGGER.error(f"Failed to send reply message: {e}")

        reply_queue.task_done()


for _ in range(1):
    create_task(process_file())
    create_task(send_reply_messages())


@Client.on_message(filters.channel & (filters.document | filters.video))
async def file_receive_handler(client: Client, message: Message):
    LOGGER.info(f"Receiver got message {message.id} from chat {message.chat.id}")

    if str(message.chat.id) not in Telegram.AUTH_CHANNEL:
        await message.reply_text("> Channel is not in AUTH_CHANNEL")
        return

    try:
        video_extensions = (".mkv", ".mp4", ".avi", ".webm", ".mov", ".flv", ".wmv")
        is_video_doc = (
            message.document
            and (
                (message.document.mime_type and message.document.mime_type.startswith("video/"))
                or (message.document.file_name and message.document.file_name.lower().endswith(video_extensions))
            )
        )

        if not (message.video or is_video_doc):
            await message.reply_text("> Not supported")
            return

        file = message.video or message.document
        title = message.caption or getattr(file, "file_name", None) or "unknown"
        msg_id = message.id
        size = get_readable_file_size(file.file_size)
        channel = str(message.chat.id).replace("-100", "")

        metadata_info = await metadata(clean_filename(title), int(channel), msg_id)
        if metadata_info is None:
            LOGGER.warning(f"Metadata failed for file: {title} (ID: {msg_id})")
            await message.reply_text(
                text="⚠️ Metadata lookup failed for this file.",
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
            return

        title = remove_urls(title)
        if not title.endswith((".mkv", ".mp4")):
            title += ".mkv"

        if Backend.USE_DEFAULT_ID:
            new_caption = (message.caption + "\n\n" + Backend.USE_DEFAULT_ID) if message.caption else Backend.USE_DEFAULT_ID
            create_task(edit_message(
                chat_id=message.chat.id,
                msg_id=message.id,
                new_caption=new_caption,
            ))

        await file_queue.put((metadata_info, int(channel), msg_id, size, title, message.chat.id, message.id))
    except FloodWait as e:
        LOGGER.info(f"Sleeping for {str(e.value)}s")
        await asleep(e.value)
        await message.reply_text(
            text=f"Got Floodwait of {str(e.value)}s",
            disable_web_page_preview=True,
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        LOGGER.error(f"Receiver error for message {message.id}: {e}")
        await message.reply_text(
            text="❌ Failed to process this file due to an internal error.",
            disable_web_page_preview=True,
            parse_mode=ParseMode.MARKDOWN,
        )
        
