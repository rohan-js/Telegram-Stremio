from asyncio import create_task, sleep as asleep, Queue, Lock
import Backend
from Backend.helper.task_manager import edit_message
from Backend.logger import LOGGER
from Backend import db
from Backend.config import Telegram
from Backend.helper.pyro import clean_filename, get_readable_file_size, remove_urls
from Backend.helper.metadata import metadata
from pyrogram import filters, Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait
from pyrogram.enums.parse_mode import ParseMode


file_queue = Queue()
db_lock = Lock()
reply_queue = Queue()  # Queue to store reply info

async def process_file():
    while True:
        metadata_info, channel, msg_id, size, title, chat_id, original_msg_id = await file_queue.get()
        async with db_lock:
            updated_id = await db.insert_media(metadata_info, channel=channel, msg_id=msg_id, size=size, name=title)
            if updated_id:
                LOGGER.info(f"{metadata_info['media_type']} updated with ID: {updated_id}")
                # Queue the reply info for sending
                await reply_queue.put((chat_id, original_msg_id, metadata_info, title, size))
            else:
                LOGGER.info("Update failed due to validation errors.")
        file_queue.task_done()

async def send_reply_messages():
    """Background task to send reply messages with stream links"""
    while True:
        chat_id, msg_id, metadata_info, title, size = await reply_queue.get()
        try:
            base_url = Telegram.BASE_URL.rstrip('/')
            imdb_id = metadata_info.get('imdb_id', '')
            media_type = metadata_info.get('media_type', 'movie')
            movie_title = metadata_info.get('title', 'Unknown')
            year = metadata_info.get('year', '')
            encoded_string = metadata_info.get('encoded_string', '')
            
            # Build the Stremio link
            if imdb_id:
                stremio_link = f"stremio://detail/{media_type}/{imdb_id}"
            else:
                stremio_link = f"{base_url}/stremio/manifest.json"
            
            # Build the direct stream link (playable in VLC/browser)
            if encoded_string:
                direct_stream = f"{base_url}/dl/{encoded_string}/video.mkv"
            else:
                direct_stream = "N/A"
            
            # Create reply message
            reply_text = (
                f"✅ **Added to Stremio!**\n\n"
                f"🎬 **{movie_title}**"
                f"{f' ({year})' if year else ''}\n"
                f"📁 Size: {size}\n"
                f"🆔 IMDB: `{imdb_id}`\n\n"
                f"▶️ **Direct Stream Link (VLC/Browser):**\n"
                f"`{direct_stream}`\n\n"
                f"🔗 **Open in Stremio:**\n"
                f"`{stremio_link}`"
            )
            
            # Import StreamBot for sending reply
            from Backend.pyrofork.bot import StreamBot
            await StreamBot.send_message(
                chat_id=chat_id,
                text=reply_text,
                reply_to_message_id=msg_id,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
            LOGGER.info(f"Sent stream link reply for: {movie_title}")
            
        except FloodWait as e:
            LOGGER.info(f"FloodWait in reply: sleeping for {e.value}s")
            await asleep(e.value)
        except Exception as e:
            LOGGER.error(f"Failed to send reply message: {e}")
        
        reply_queue.task_done()

# Start background tasks
for _ in range(1):
    create_task(process_file())
    create_task(send_reply_messages())


@Client.on_message(filters.channel & (filters.document | filters.video))
async def file_receive_handler(client: Client, message: Message):
    if str(message.chat.id) in Telegram.AUTH_CHANNEL:
        try:
            if message.video or (message.document and message.document.mime_type.startswith("video/")):
                file = message.video or message.document
                title = message.caption or file.file_name
                msg_id = message.id
                size = get_readable_file_size(file.file_size)
                channel = str(message.chat.id).replace("-100", "")

                metadata_info = await metadata(clean_filename(title), int(channel), msg_id)
                if metadata_info is None:
                    LOGGER.warning(f"Metadata failed for file: {title} (ID: {msg_id})")
                    return

                title = remove_urls(title)
                if not title.endswith(('.mkv', '.mp4')):
                    title += '.mkv'

                if Backend.USE_DEFAULT_ID:
                    new_caption = (message.caption + "\n\n" + Backend.USE_DEFAULT_ID) if message.caption else Backend.USE_DEFAULT_ID
                    create_task(edit_message(
                        chat_id=message.chat.id,
                        msg_id=message.id,
                        new_caption=new_caption
                    ))

                # Pass chat_id and original msg_id for reply
                await file_queue.put((metadata_info, int(channel), msg_id, size, title, message.chat.id, message.id))
            else:
                await message.reply_text("> Not supported")
        except FloodWait as e:
            LOGGER.info(f"Sleeping for {str(e.value)}s")
            await asleep(e.value)
            await message.reply_text(
                text=f"Got Floodwait of {str(e.value)}s",
                disable_web_page_preview=True,
                parse_mode=ParseMode.MARKDOWN
            )
    else:
        await message.reply_text("> Channel is not in AUTH_CHANNEL")
        
