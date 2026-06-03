from asyncio import Queue, Lock, create_task, sleep as asleep
from html import escape

import Backend
from pyrogram import Client, filters
from pyrogram.enums.parse_mode import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from Backend import db
from Backend.config import Telegram
from Backend.helper.encrypt import encode_string
from Backend.helper.metadata import metadata, extract_default_id
from Backend.helper.pyro import clean_filename, get_readable_file_size, remove_urls
from Backend.helper.task_manager import edit_message
from Backend.helper.torrent_source import (
    TorrentItem,
    extract_magnet_links,
    parse_magnet,
    parse_torrent,
)
from Backend.helper.torrent_downloads import (
    TORRENT_DOWNLOAD_MANAGER,
    torrent_download_callback_data,
)
from Backend.helper.watch_links import (
    callback_data_fits,
    telegram_user_display_name,
    watch_callback_data,
)
from Backend.logger import LOGGER
from Backend.helper.disk_cache import PRECACHE_MANAGER, PrecacheJob


file_queue = Queue()
db_lock = Lock()
reply_queue = Queue()

METADATA_FAILED_TEXT = (
    "Metadata failed for this file. The filename may be valid, but the external metadata service "
    "may be temporarily unavailable. Try again after a minute, or add an IMDb/TMDb link in the caption. "
    "For TV files, use S01E01 for episodes or S01 COMBINED for a full-season pack."
)

TORRENT_METADATA_FAILED_TEXT = (
    "Metadata failed for this torrent. Add an IMDb/TMDb link, or use a clearer title like "
    "Movie Name 2024 1080p / Show.Name.S01E01. For full-season torrents, episode filenames "
    "should include S01E01 style numbering."
)

VIDEO_EXTENSIONS = (".mkv", ".mp4", ".avi", ".webm", ".mov", ".flv", ".wmv")


def _telegram_user_name(user) -> str:
    if not user:
        return "Telegram User"
    return telegram_user_display_name(
        getattr(user, "first_name", None),
        getattr(user, "last_name", None),
        getattr(user, "username", None),
        getattr(user, "id", None),
    )


def _requester_from_callback(callback_query: CallbackQuery) -> dict:
    user = callback_query.from_user
    if not user:
        return {}
    return {
        "user_id": user.id,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "username": user.username,
        "name": _telegram_user_name(user),
    }


async def _edit_status(chat_id: int | None, msg_id: int | None, text: str) -> None:
    if not chat_id or not msg_id:
        return
    try:
        from Backend.pyrofork.bot import StreamBot
        await StreamBot.edit_message_text(
            chat_id=int(chat_id),
            message_id=int(msg_id),
            text=text,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        LOGGER.debug(f"Could not edit ingest status message {msg_id}: {e}")


def _format_queue_status(state: str, title: str, position: int | None = None, reason: str | None = None) -> str:
    parts = [f"📥 <b>Ingestion {escape(state)}</b>", f"<code>{escape(title)}</code>"]
    if position:
        parts.append(f"Queue position: <code>{position}</code>")
    if reason:
        parts.append(f"Reason: <code>{escape(reason[:220])}</code>")
    return "\n".join(parts)


async def _metadata_for_job(job: dict) -> dict | None:
    title = job.get("title") or job.get("file_name") or "unknown"
    return await metadata(
        clean_filename(title),
        int(job["channel"]),
        int(job["msg_id"]),
        override_id=job.get("override_id"),
    )


async def _process_ingest_job(job: dict) -> None:
    title = job.get("title") or job.get("file_name") or "unknown"
    await _edit_status(
        job.get("status_chat_id"),
        job.get("status_msg_id"),
        _format_queue_status("processing", title),
    )

    metadata_info = await _metadata_for_job(job)
    if metadata_info is None:
        reason = "metadata_failed"
        await db.upsert_unmatched_media(job, reason)
        await _edit_status(
            job.get("status_chat_id"),
            job.get("status_msg_id"),
            _format_queue_status("failed", title, reason=reason),
        )
        if job.get("source_type") == "torrent":
            await job["message"].reply_text(TORRENT_METADATA_FAILED_TEXT)
        else:
            await job["message"].reply_text(METADATA_FAILED_TEXT)
        LOGGER.warning(f"Metadata failed for queued {job.get('source_type')} item: {title} (ID: {job.get('msg_id')})")
        return

    if job.get("source_type") == "torrent":
        torrent = job.get("torrent") or {}
        encoded_string = await encode_string({
            "source_type": "torrent",
            "chat_id": int(job["channel"]),
            "msg_id": int(job["msg_id"]),
            "info_hash": torrent.get("info_hash"),
            "file_idx": torrent.get("file_idx"),
            "name": torrent.get("file_name"),
        })
        metadata_info.update({
            "source_type": "torrent",
            "encoded_string": encoded_string,
            "info_hash": torrent.get("info_hash"),
            "file_idx": torrent.get("file_idx"),
            "sources": torrent.get("sources") or [],
            "filename": torrent.get("file_name") or title,
            "video_size": torrent.get("video_size"),
            "origin_chat_id": int(job["chat_id"]),
            "origin_msg_id": int(job["msg_id"]),
            "torrent_private": bool(torrent.get("torrent_private", False)),
            "torrent_source_uri": torrent.get("torrent_source_uri"),
            "torrent_file_chat_id": torrent.get("torrent_file_chat_id"),
            "torrent_file_msg_id": torrent.get("torrent_file_msg_id"),
        })

    updated_id = await db.insert_media(
        metadata_info,
        channel=int(job["channel"]),
        msg_id=int(job["msg_id"]),
        size=job.get("size") or "",
        name=job.get("display_name") or title,
    )
    if updated_id:
        LOGGER.info(f"{metadata_info['media_type']} updated with ID: {updated_id}")
        await _edit_status(
            job.get("status_chat_id"),
            job.get("status_msg_id"),
            _format_queue_status("indexed", title),
        )
        await reply_queue.put((job["chat_id"], job["original_msg_id"], metadata_info, job.get("display_name") or title, job.get("size") or ""))
    else:
        reason = "database_insert_failed"
        await db.upsert_unmatched_media(job, reason)
        await _edit_status(
            job.get("status_chat_id"),
            job.get("status_msg_id"),
            _format_queue_status("failed", title, reason=reason),
        )
        LOGGER.info("Queued update failed due to validation/database errors.")


async def process_file():
    while True:
        job = await file_queue.get()
        try:
            async with db_lock:
                if isinstance(job, dict):
                    await _process_ingest_job(job)
                else:
                    metadata_info, channel, msg_id, size, title, chat_id, original_msg_id = job
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
        except Exception as e:
            LOGGER.exception(f"Ingestion worker failed: {e}")
        finally:
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
            source_type = metadata_info.get("source_type", "telegram")

            if imdb_id:
                if media_type == "tv":
                    season = metadata_info.get("season_number", 1)
                    episode = metadata_info.get("episode_number", 1)
                    stremio_link = f"{base_url}/stremio/open/series/{imdb_id}?season={season}&episode={episode}"
                else:
                    stremio_link = f"{base_url}/stremio/open/movie/{imdb_id}"
            else:
                stremio_link = f"{base_url}/stremio/{token}/configure" if token else f"{base_url}/stremio"

            if source_type == "telegram" and encoded_string and token:
                direct_stream = f"{base_url}/dl/{token}/{encoded_string}/video.mkv"
                vlc_page = f"{base_url}/vlc/{token}/{encoded_string}"
            else:
                direct_stream = "N/A"
                vlc_page = None

            rating_str = f"⭐ {rating}" if rating else ""
            if source_type == "torrent":
                help_note = "🧲 Torrent stream. Playback speed depends on seeders/peers.\n\n"
                stream_note = ""
            else:
                help_note = (
                    "⚠️ If streaming is slow or not loading, turn on 🌐 Cloudflare WARP and try again.\n\n"
                    "⏪ If seeking/jumping through the video goes back to the beginning on 📺 TV or 📱 mobile. "
                    "Try another file or use VLC Player / Windows\n\n"
                    "🛠️ Facing any issues? Just type them here and I'll help!\n\n"
                )
                stream_note = f"▶️ <b>Direct Stream Link:</b>\n<code>{direct_stream}</code>"
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
                        f"{stream_note}"
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
                        f"{stream_note}"
                    )
            else:
                reply_text = (
                    f"🎬 <b>{movie_title}</b>"
                    f"{f' ({year})' if year else ''}\n"
                    f"{rating_str}"
                    f"{f' | {quality}' if quality else ''}"
                    f" | {size}\n\n"
                    f"{help_note}"
                    f"{stream_note}"
                )

            buttons_row = []
            try:
                watch_request_id = await db.create_watch_link_request({
                    "stremio_link": stremio_link,
                    "media_title": movie_title,
                    "media_type": media_type,
                    "imdb_id": imdb_id,
                    "season_number": metadata_info.get("season_number"),
                    "episode_number": metadata_info.get("episode_number"),
                    "source_type": source_type,
                    "origin_chat_id": chat_id,
                    "origin_msg_id": msg_id,
                })
                watch_data = watch_callback_data(watch_request_id)
                if callback_data_fits(watch_data):
                    buttons_row.append(InlineKeyboardButton("▶️ Watch in Stremio", callback_data=watch_data))
                else:
                    buttons_row.append(InlineKeyboardButton("▶️ Watch in Stremio", url=stremio_link))
            except Exception as e:
                LOGGER.warning(f"Could not create watch callback link, falling back to direct URL: {e}")
                buttons_row.append(InlineKeyboardButton("▶️ Watch in Stremio", url=stremio_link))

            if vlc_page:
                buttons_row.append(InlineKeyboardButton("🎬 Watch in VLC", url=vlc_page))
            button_rows = [buttons_row]
            if source_type == "torrent" and metadata_info.get("info_hash"):
                button_rows.append([
                    InlineKeyboardButton(
                        "⬇️ Download to VPS",
                        callback_data=torrent_download_callback_data(metadata_info["info_hash"]),
                    )
                ])
            buttons = InlineKeyboardMarkup(button_rows)

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


def _is_video_document(message: Message) -> bool:
    return bool(
        message.document and (
            (message.document.mime_type and message.document.mime_type.startswith("video/")) or
            (message.document.file_name and message.document.file_name.lower().endswith(VIDEO_EXTENSIONS))
        )
    )


def _is_torrent_document(message: Message) -> bool:
    return bool(
        message.document and (
            (message.document.file_name and message.document.file_name.lower().endswith(".torrent")) or
            message.document.mime_type == "application/x-bittorrent"
        )
    )


def _message_text(message: Message) -> str:
    return message.text or message.caption or ""


def _torrent_title(item: TorrentItem, text: str) -> str:
    title = item.file_name or item.display_name
    if title and title != item.info_hash:
        return title

    cleaned_text = text
    for magnet in extract_magnet_links(text):
        cleaned_text = cleaned_text.replace(magnet, " ")
    cleaned_text = remove_urls(cleaned_text).strip()
    return cleaned_text or item.display_name


@Client.on_callback_query(filters.regex(r"^tdl_([a-fA-F0-9]{40})$"))
async def torrent_download_callback(client: Client, callback_query: CallbackQuery):
    try:
        info_hash = callback_query.matches[0].group(1).lower()
        requester_id = callback_query.from_user.id if callback_query.from_user else None
        message = callback_query.message
        if not message:
            return await callback_query.answer("Missing status message.", show_alert=True)

        ok, text, _ = await TORRENT_DOWNLOAD_MANAGER.queue_from_info_hash(
            client=client,
            info_hash=info_hash,
            requester_user_id=requester_id,
            status_message_chat_id=message.chat.id,
            status_message_id=message.id,
        )
        await callback_query.answer(text, show_alert=not ok)
    except Exception as e:
        LOGGER.error(f"Torrent download callback failed: {e}")
        await callback_query.answer("Could not queue torrent download.", show_alert=True)


@Client.on_callback_query(filters.regex(r"^watch_([A-Za-z0-9_-]{6,24})$"))
async def watch_link_callback(client: Client, callback_query: CallbackQuery):
    request_id = callback_query.matches[0].group(1)
    requester = _requester_from_callback(callback_query)
    if not requester.get("user_id"):
        return await callback_query.answer("Could not identify your Telegram account.", show_alert=True)

    try:
        watch_request = await db.mark_watch_link_requested(request_id, requester)
        if not watch_request:
            return await callback_query.answer("This watch link expired. Please use a newer post.", show_alert=True)

        await db.update_user_interaction(
            int(requester["user_id"]),
            requester.get("first_name") or requester.get("name") or f"User {requester['user_id']}",
            requester.get("username"),
        )

        title = watch_request.get("media_title") or "this title"
        media_type = watch_request.get("media_type") or "media"
        season = watch_request.get("season_number")
        episode = watch_request.get("episode_number")
        episode_text = ""
        if media_type == "tv" and season and episode:
            episode_text = f"\n📺 S{int(season):02d}E{int(episode):02d}"

        await client.send_message(
            chat_id=int(requester["user_id"]),
            text=(
                f"▶️ <b>Watch in Stremio</b>\n\n"
                f"🎬 <b>{escape(str(title))}</b>{episode_text}\n\n"
                "Tap the button below to open this title in Stremio."
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️ Watch in Stremio", url=watch_request["stremio_link"])]
            ]),
            disable_web_page_preview=True,
        )
        await db.mark_watch_link_delivery(request_id, "sent")
        await callback_query.answer("I sent the Stremio link in your DM.", show_alert=False)
    except Exception as e:
        LOGGER.warning(f"Could not deliver watch link callback {request_id}: {e}")
        try:
            await db.mark_watch_link_delivery(request_id, "failed", str(e))
        except Exception:
            pass
        await callback_query.answer("Please start the bot first, then tap Watch again.", show_alert=True)


async def _queue_torrent_item(message: Message, item: TorrentItem, override_id: str | None) -> bool:
    channel = int(str(message.chat.id).replace("-100", ""))
    msg_id = message.id
    raw_text = _message_text(message)
    title = _torrent_title(item, raw_text)
    if not title or title == item.info_hash:
        return False

    display_title = remove_urls(item.file_name or title)
    if display_title and not display_title.lower().endswith(VIDEO_EXTENSIONS):
        display_title += ".mkv"

    position = file_queue.qsize() + 1
    status_msg = await message.reply_text(
        _format_queue_status("queued", display_title or title, position=position),
        parse_mode=ParseMode.HTML,
    )
    await file_queue.put({
        "source_type": "torrent",
        "source_key": f"torrent:{message.chat.id}:{msg_id}:{item.info_hash}:{item.file_idx}",
        "item_key": f"{item.info_hash}:{item.file_idx}",
        "channel": channel,
        "chat_id": int(message.chat.id),
        "msg_id": msg_id,
        "original_msg_id": message.id,
        "status_chat_id": status_msg.chat.id,
        "status_msg_id": status_msg.id,
        "title": title,
        "file_name": item.file_name or display_title,
        "display_name": display_title or title,
        "size": item.size_text,
        "override_id": override_id,
        "message": message,
        "torrent": {
            "info_hash": item.info_hash,
            "file_idx": item.file_idx,
            "sources": item.sources,
            "file_name": item.file_name or display_title,
            "video_size": item.size_bytes,
            "torrent_private": bool(item.is_private),
            "torrent_source_uri": item.source_uri,
            "torrent_file_chat_id": int(message.chat.id) if not item.source_uri and _is_torrent_document(message) else None,
            "torrent_file_msg_id": int(msg_id) if not item.source_uri and _is_torrent_document(message) else None,
        },
    })
    db.queue_torrent_stats_refresh(
        item.info_hash,
        item.sources,
        torrent_private=bool(item.is_private),
    )
    return True


async def _handle_torrent_message(client: Client, message: Message) -> bool:
    text = _message_text(message)
    override_id = extract_default_id(text) if text else None
    items: list[TorrentItem] = []

    for magnet in extract_magnet_links(text):
        try:
            items.append(parse_magnet(magnet, fallback_name=None))
        except Exception as e:
            LOGGER.warning(f"Failed to parse magnet in message {message.id}: {e}")

    if _is_torrent_document(message):
        try:
            torrent_file = await client.download_media(message, in_memory=True)
            torrent_file.seek(0)
            items.extend(parse_torrent(torrent_file.read()))
            torrent_file.close()
        except Exception as e:
            LOGGER.warning(f"Failed to parse torrent document {message.id}: {e}")

    if not items:
        return False

    queued = 0
    for item in items:
        if await _queue_torrent_item(message, item, override_id):
            queued += 1

    if queued == 0:
        await message.reply_text(TORRENT_METADATA_FAILED_TEXT)
        return True

    LOGGER.info(f"Queued {queued} torrent stream(s) from message {message.id}.")
    return True


@Client.on_message(filters.channel & (filters.document | filters.video | filters.text))
async def file_receive_handler(client: Client, message: Message):
    if str(message.chat.id) in Telegram.AUTH_CHANNEL:
        try:
            if await _handle_torrent_message(client, message):
                return

            is_video_doc = _is_video_document(message)
            if message.video or is_video_doc:
                file = message.video or message.document
                file_name = getattr(file, "file_name", None) or ""
                if file_name and file_name.lower().endswith(VIDEO_EXTENSIONS):
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

                title = remove_urls(title)
                if not title.endswith((".mkv", ".mp4")):
                    title += ".mkv"

                if Backend.USE_DEFAULT_ID:
                    new_caption = (message.caption + "\n\n" + Backend.USE_DEFAULT_ID) if message.caption else Backend.USE_DEFAULT_ID
                    create_task(edit_message(chat_id=message.chat.id, msg_id=message.id, new_caption=new_caption))

                position = file_queue.qsize() + 1
                status_msg = await message.reply_text(
                    _format_queue_status("queued", title, position=position),
                    parse_mode=ParseMode.HTML,
                )
                await file_queue.put({
                    "source_type": "telegram",
                    "source_key": f"telegram:{message.chat.id}:{msg_id}",
                    "channel": int(channel),
                    "chat_id": int(message.chat.id),
                    "msg_id": int(msg_id),
                    "original_msg_id": message.id,
                    "status_chat_id": status_msg.chat.id,
                    "status_msg_id": status_msg.id,
                    "title": title,
                    "file_name": file_name or title,
                    "display_name": title,
                    "size": size,
                    "file_size": int(getattr(file, "file_size", 0) or 0),
                    "message": message,
                })
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


@Client.on_edited_message(filters.channel & (filters.document | filters.video | filters.text))
async def file_edited_handler(client: Client, message: Message):
    if str(message.chat.id) in Telegram.AUTH_CHANNEL:
        try:
            if await _handle_torrent_message(client, message):
                return

            if message.video or _is_video_document(message):
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
                    title = remove_urls(title)
                    if not title.endswith((".mkv", ".mp4")):
                        title += ".mkv"
                    position = file_queue.qsize() + 1
                    status_msg = await message.reply_text(
                        _format_queue_status("queued", title, position=position),
                        parse_mode=ParseMode.HTML,
                    )
                    await file_queue.put({
                        "source_type": "telegram",
                        "source_key": f"telegram:{message.chat.id}:{msg_id}",
                        "channel": int(channel),
                        "chat_id": int(message.chat.id),
                        "msg_id": int(msg_id),
                        "original_msg_id": message.id,
                        "status_chat_id": status_msg.chat.id,
                        "status_msg_id": status_msg.id,
                        "title": title,
                        "file_name": getattr(file, "file_name", None) or title,
                        "display_name": title,
                        "size": size,
                        "file_size": int(getattr(file, "file_size", 0) or 0),
                        "override_id": override_id,
                        "message": message,
                    })
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
                    deleted = await db.delete_media_by_origin(int(message.chat.id), int(msg_id))
                    if not deleted:
                        deleted = await db.delete_media_by_stream_id(stream_id_hash)
                    if deleted:
                        LOGGER.info(f"Automatically purged deleted message {msg_id} from database.")
                except Exception as ex:
                    LOGGER.error(f"Failed to scrub deleted message {msg_id}: {ex}")
    except Exception as e:
        LOGGER.error(f"Error handling deleted messages: {e}")
