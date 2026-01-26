from pyrogram.file_id import FileId
from typing import Optional
from Backend.logger import LOGGER
from Backend import __version__, now, timezone
from Backend.config import Telegram
from Backend.helper.exceptions import FIleNotFound
from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, remove as aioremove
from pyrogram import Client
from Backend.pyrofork.bot import StreamBot
import re
from pyrogram.types import BotCommand
from pyrogram import enums


def is_media(message):
    return next((getattr(message, attr) for attr in ["document", "photo", "video", "audio", "voice", "video_note", "sticker", "animation"] if getattr(message, attr)), None)


async def get_file_ids(client: Client, chat_id: int, message_id: int) -> Optional[FileId]:
    try:
        message = await client.get_messages(chat_id, message_id)
        if message.empty:
            raise FIleNotFound("Message not found or empty")
        
        if media := is_media(message):
            file_id_obj = FileId.decode(media.file_id)
            file_unique_id = media.file_unique_id
            
            setattr(file_id_obj, 'file_name', getattr(media, 'file_name', ''))
            setattr(file_id_obj, 'file_size', getattr(media, 'file_size', 0))
            setattr(file_id_obj, 'mime_type', getattr(media, 'mime_type', ''))
            setattr(file_id_obj, 'unique_id', file_unique_id)
            
            return file_id_obj
        else:
            raise FIleNotFound("No supported media found in message")
    except Exception as e:
        LOGGER.error(f"Error getting file IDs: {e}")
        raise
        


def get_readable_file_size(size_in_bytes):
    size_in_bytes = int(size_in_bytes) if str(size_in_bytes).isdigit() else 0
    if not size_in_bytes:
        return '0B'
    
    index, SIZE_UNITS = 0, ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    while size_in_bytes >= 1024 and index < len(SIZE_UNITS) - 1:
        size_in_bytes /= 1024
        index += 1
    
    return f'{size_in_bytes:.2f}{SIZE_UNITS[index]}' if index > 0 else f'{size_in_bytes:.0f}B'


def clean_filename(filename):
    """
    Enhanced filename cleaner for messy Telegram filenames.
    Removes emojis, channel watermarks, promotional text, and normalizes for PTN parsing.
    """
    if not filename:
        return "unknown_file"
    
    cleaned = filename
    
    # 1. Remove emojis and special unicode symbols
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags
        "\U00002702-\U000027B0"  # dingbats
        "\U000024C2-\U0001F251"  # misc
        "\U0001F900-\U0001F9FF"  # supplemental symbols
        "\U0001FA00-\U0001FA6F"  # chess symbols
        "\U0001FA70-\U0001FAFF"  # symbols extended
        "\U00002600-\U000026FF"  # misc symbols
        "\U00002700-\U000027BF"  # dingbats
        "\U0001F018-\U0001F0FF"  # playing cards and more
        "\U0001F170-\U0001F1FF"  # enclosed characters
        "\U0001F004"  # mahjong
        "\U0001F0CF"  # playing card
        "\U0001F18E"  # AB button
        "\U00003030"  # wavy dash
        "\U000000A9"  # copyright
        "\U000000AE"  # registered
        "\U0000203C-\U00003299"  # misc symbols
        "🎗️🎬🎥📺🎞️🎦🔥💥⚡✨🌟⭐💫🎭🏆🔴🟢🟡❤️💙💚💛🧡💜🖤🤍🤎🚫⛔"  # common media emojis
        "]+"
    , re.UNICODE)
    cleaned = emoji_pattern.sub(' ', cleaned)
    
    # 2. Remove release group tags in brackets at start: [CK], [MX], [TG], etc.
    cleaned = re.sub(r'^\s*\[[A-Z]{1,4}\]\s*[-:]?\s*', '', cleaned, flags=re.IGNORECASE)
    
    # 3. Remove common promotional prefixes/suffixes in brackets
    # Handles: [JOIN NOW @CHANNEL], [Subscribe @xyz], etc.
    bracket_promo = re.compile(
        r'\[.*?(?:JOIN|SUBSCRIBE|DOWNLOAD|GET|VISIT|FROM|@|telegram|channel|group|NOW).*?\]',
        re.IGNORECASE
    )
    cleaned = bracket_promo.sub(' ', cleaned)
    
    # 4. Remove standalone promotional phrases (not in brackets)
    promo_phrases = re.compile(
        r'(?:^|\s)(?:JOIN\s*NOW|SUBSCRIBE\s*NOW|DOWNLOAD\s*NOW|GET\s*NOW|'
        r'VISIT\s*NOW|JOIN\s*US|SUBSCRIBE\s*TO|DOWNLOAD\s*FROM|'
        r'@\w+|FROM\s*@\w+|POWERED\s*BY|PRESENTED\s*BY|'
        r'NMX\s*NAVARASA\s*SIGMA\s*IBA\s*WEBSERIES|'
        r'NMX\s*NAVARASA|SIGMA\s*IBA)[\s\-:]*',
        re.IGNORECASE
    )
    cleaned = promo_phrases.sub(' ', cleaned)
    
    # 5. Remove @username patterns anywhere
    cleaned = re.sub(r'@[A-Za-z0-9_]+', '', cleaned)
    
    # 6. Remove common Telegram channel watermarks in various formats
    watermark_pattern = r'_@[A-Za-z0-9]+_|@[A-Za-z0-9]+_|[\[\]\s@]*@[^.\s\[\]]+[\]\[\s@]*'
    cleaned = re.sub(watermark_pattern, ' ', cleaned)
    
    # 7. Remove language and quality tags that don't help with title matching
    cleaned = re.sub(
        r'\b(?:Telugu|Tamil|Hindi|Malayalam|Kannada|Bengali|Marathi|Punjabi|'
        r'English|Dubbed|Dual\s*Audio|Multi\s*Audio|'
        r'HQ|HDRi|HDRip|HDR|WEB-DL|WEBRip|BluRay|BRRip|DVDRip|HDTV|'
        r'CAMRip|HDCAM|HDCAMRip|PreDVD|DVDScr|'
        r'720p|1080p|2160p|4K|UHD|FHD|HD|SD|'
        r'x264|x265|HEVC|H\.264|H\.265|AVC|'
        r'AAC|AC3|DTS|MP3|FLAC|'
        r'ESub|ESubs|HardSub|SoftSub|SubsIncluded)\b',
        ' ', cleaned, flags=re.IGNORECASE
    )
    
    # 8. Remove streaming service and release group tags
    cleaned = re.sub(
        r'\b(?:org|AMZN|Amazon|NF|Netflix|DDP|DD|TVDL|HDHub4u|'
        r'HDHub|FilmCorner|MovieHub|TvShows|WebSeries|FILMCORNERMAIN|'
        r'YTS|YIFY|RARBG|PSA|Pahe|TamilRockers|Tamilmv|'
        r'5\.1|2\.1|2\.0|7\.0|7\.1|5\.0|~|\d+kbps|'
        r'CK|MX|TG|MKVKING|MkvHub)\b',
        ' ', cleaned, flags=re.IGNORECASE
    )
    
    # 9. Remove brackets that only contain junk (emojis, @mentions, etc.)
    cleaned = re.sub(r'\[\s*\]|\(\s*\)', '', cleaned)
    
    # 10. Clean up parentheses with junk but preserve year markers like (2025)
    cleaned = re.sub(r'\(\s*(?!\d{4}\s*\))[^)]*@[^)]*\)', '', cleaned)
    
    # 11. Remove leading/trailing special characters and normalize spaces
    cleaned = re.sub(r'^[\s\-_.:]+|[\s\-_.:]+$', '', cleaned)
    cleaned = re.sub(r'[\s\-_.]+', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # 12. If filename starts with common junk words, remove them
    junk_start = re.compile(
        r'^(?:DOWNLOAD|WATCH|STREAM|NEW|LATEST|HD|FULL|FREE)[\s\-:]+',
        re.IGNORECASE
    )
    cleaned = junk_start.sub('', cleaned)
    
    # 13. Restore periods before file extensions
    cleaned = cleaned.replace(' .', '.')
    
    # 14. Remove file extension for cleaner title matching
    cleaned = re.sub(r'\.(mkv|mp4|avi|mov|wmv|flv|webm)$', '', cleaned, flags=re.IGNORECASE)
    
    # 15. Final cleanup - ensure we have something useful
    cleaned = cleaned.strip()
    
    return cleaned if cleaned and len(cleaned) > 2 else "unknown_file"


def get_readable_time(seconds: int) -> str:
    count = 0
    readable_time = ""
    time_list = []
    time_suffix_list = ["s", "m", "h", " days"]
    
    while count < 4:
        count += 1
        if count < 3:
            remainder, result = divmod(seconds, 60)
        else:
            remainder, result = divmod(seconds, 24)
        
        if seconds == 0 and remainder == 0:
            break
        
        time_list.append(int(result))
        seconds = int(remainder)
    
    for x in range(len(time_list)):
        time_list[x] = str(time_list[x]) + time_suffix_list[x]
    
    if len(time_list) == 4:
        readable_time += time_list.pop() + ", "
    
    time_list.reverse()
    readable_time += ": ".join(time_list)
    
    return readable_time



def remove_urls(text):
    if not text:
        return ""
    
    url_pattern = r'\b(?:https?|ftp):\/\/[^\s/$.?#].[^\s]*'
    text_without_urls = re.sub(url_pattern, '', text)
    cleaned_text = re.sub(r'\s+', ' ', text_without_urls).strip()
    
    return cleaned_text



async def restart_notification():
    chat_id, msg_id = 0, 0
    try:
        if await aiopath.exists(".restartmsg"):
            async with aiopen(".restartmsg", "r") as f:
                data = await f.readlines()
                chat_id, msg_id = map(int, data)
            
            try:
                repo = Telegram.UPSTREAM_REPO.split('/')
                UPSTREAM_REPO = f"https://github.com/{repo[-2]}/{repo[-1]}"
                await StreamBot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=f"... ♻️ Restart Successfully...! \n\nDate: {now.strftime('%d/%m/%y')}\nTime: {now.strftime('%I:%M:%S %p')}\nTimeZone: {timezone.zone}\n\nRepo: {UPSTREAM_REPO}\nBranch: {Telegram.UPSTREAM_BRANCH}\nVersion: {__version__}",
                    parse_mode=enums.ParseMode.HTML
                )
            except Exception as e:
                LOGGER.error(f"Failed to edit restart message: {e}")
            
            await aioremove(".restartmsg")
            
    except Exception as e:
        LOGGER.error(f"Error in restart_notification: {e}")


# Bot commands
commands = [
    BotCommand("start", "🚀 Start the bot"),
    BotCommand("set", "🎬 Manually add IMDb metadata"),
    # BotCommand("fixmetadata", "⚙️ Fix empty fields of Metadata"),
    BotCommand("log", "📄 Send the log file"),
    BotCommand("restart", "♻️ Restart the bot"),
]


async def setup_bot_commands(bot: Client):
    try:
        current_commands = await bot.get_bot_commands()
        if current_commands:
            LOGGER.info(f"Found {len(current_commands)} existing commands. Deleting them...")
            await bot.set_bot_commands([])
        
        await bot.set_bot_commands(commands)
        LOGGER.info("Bot commands updated successfully.")
    except Exception as e:
        LOGGER.error(f"Error setting up bot commands: {e}")

