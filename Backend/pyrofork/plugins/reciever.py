from asyncio import Queue, Lock, create_task, sleep as asleep
from html import escape
from time import monotonic

import Backend
from pyrogram import Client, filters
from pyrogram.enums.parse_mode import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from Backend import db
from Backend.config import Telegram
from Backend.helper.encrypt import encode_string
from Backend.helper.metadata import metadata, extract_default_id, pop_match_failure
from Backend.helper.pyro import clean_filename, finalize_media_name, get_readable_file_size, remove_urls
from Backend.helper.split_files import VIDEO_EXTENSIONS as MEDIA_VIDEO_EXTENSIONS, parse_split_info
from Backend.helper.manual_add import resolve_telegram_message
from Backend.helper.manual_session import manual_session_manager
from Backend.helper.settings_manager import SettingsManager
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
from Backend.helper.announcer import announce_new_media
from Backend.helper.requests_manager import mark_uploaded_for_media
from Backend.helper.subtitles import ingest_subtitle, is_subtitle_file, remove_subtitle


file_queue = Queue()
db_lock = Lock()
reply_queue = Queue()
STATUS_EDIT_MIN_INTERVAL = 1.0

METADATA_FAILED_TEXT = (
    "⚠️ <b>Metadata failed</b>\n"
    "<code>{title}</code>\n\n"
    "I could not find any usable movie/series metadata for this file. "
    "Please resend it with an IMDb/TMDb link in the caption, or use a clearer filename like "
    "<code>Movie Name 2024 1080p</code> / <code>Show.Name.S01E01</code>."
)

TORRENT_METADATA_FAILED_TEXT = (
    "⚠️ <b>Metadata failed</b>\n"
    "<code>{title}</code>\n\n"
    "I could not find any usable movie/series metadata for this torrent. "
    "Please resend it with an IMDb/TMDb link in the caption, or use a clearer torrent/file name like "
    "<code>Movie Name 2024 1080p</code> / <code>Show.Name.S01E01</code>."
)

VIDEO_EXTENSIONS = tuple(f".{extension}" for extension in MEDIA_VIDEO_EXTENSIONS)


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


async def _delete_status(chat_id: int | None, msg_id: int | None) -> None:
    if not chat_id or not msg_id:
        return
    try:
        from Backend.pyrofork.bot import StreamBot
        await StreamBot.delete_messages(chat_id=int(chat_id), message_ids=int(msg_id))
    except Exception as e:
        LOGGER.debug(f"Could not delete ingest status message {msg_id}: {e}")


async def _send_status_message(message: Message, title: str, position: int | None) -> tuple[int | None, int | None]:
    try:
        sent = await message.reply_text(
            _format_queue_status("queued", title, position=position),
            quote=True,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return int(sent.chat.id), int(sent.id)
    except FloodWait as e:
        LOGGER.info(f"FloodWait in ingest status reply: sleeping for {e.value}s")
        await asleep(e.value)
        return await _send_status_message(message, title, position)
    except Exception as e:
        LOGGER.debug(f"Could not create ingest status message for {getattr(message, 'id', None)}: {e}")
        return None, None


def _format_queue_status(state: str, title: str, position: int | None = None, reason: str | None = None) -> str:
    parts = [f"📥 <b>Ingestion {escape(state)}</b>", f"<code>{escape(title)}</code>"]
    if position:
        parts.append(f"Queue position: <code>{position}</code>")
    if reason:
        parts.append(f"Reason: <code>{escape(reason[:220])}</code>")
    return "\n".join(parts)


async def _set_status(job: dict, state: str, reason: str | None = None, force: bool = False) -> None:
    title = job.get("display_name") or job.get("title") or job.get("file_name") or "unknown"
    text = _format_queue_status(state, title, position=job.get("queue_position"), reason=reason)
    if text == job.get("_last_status_text"):
        return
    now = monotonic()
    last_at = float(job.get("_last_status_at") or 0)
    if not force and last_at and now - last_at < STATUS_EDIT_MIN_INTERVAL:
        await asleep(STATUS_EDIT_MIN_INTERVAL - (now - last_at))
    await _edit_status(job.get("status_chat_id"), job.get("status_msg_id"), text)
    job["_last_status_text"] = text
    job["_last_status_at"] = monotonic()


async def _finalize_failed_status_or_reply(job: dict, reason: str) -> None:
    if job.get("status_chat_id") and job.get("status_msg_id"):
        title = job.get("display_name") or job.get("title") or job.get("file_name") or "unknown"
        template = TORRENT_METADATA_FAILED_TEXT if job.get("source_type") == "torrent" else METADATA_FAILED_TEXT
        text = f"{template.format(title=escape(title))}\n\nReason: <code>{escape(str(reason)[:120])}</code>"
        await _edit_status(job.get("status_chat_id"), job.get("status_msg_id"), text)
        job["_last_status_text"] = text
        job["_last_status_at"] = monotonic()
        return
    await _send_metadata_failed_reply(job, reason)


async def _enqueue_ingest_job(message: Message, job: dict) -> None:
    title = job.get("display_name") or job.get("title") or job.get("file_name") or "unknown"
    position = file_queue.qsize() + 1
    status_chat_id, status_msg_id = await _send_status_message(message, title, position)
    job.update({
        "queue_position": position,
        "status_chat_id": status_chat_id,
        "status_msg_id": status_msg_id,
        "_last_status_text": _format_queue_status("queued", title, position=position) if status_msg_id else None,
        "_last_status_at": monotonic() if status_msg_id else 0,
    })
    await file_queue.put(job)


async def _send_metadata_failed_reply(job: dict, reason: str) -> None:
    chat_id = job.get("chat_id")
    msg_id = job.get("original_msg_id") or job.get("msg_id")
    if not chat_id or not msg_id:
        return

    title = job.get("display_name") or job.get("title") or job.get("file_name") or "unknown"
    template = TORRENT_METADATA_FAILED_TEXT if job.get("source_type") == "torrent" else METADATA_FAILED_TEXT
    text = f"{template.format(title=escape(title))}\n\nReason: <code>{escape(str(reason)[:120])}</code>"

    try:
        from Backend.pyrofork.bot import StreamBot
        await StreamBot.send_message(
            chat_id=int(chat_id),
            text=text,
            reply_to_message_id=int(msg_id),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except FloodWait as e:
        LOGGER.info(f"FloodWait in metadata failure reply: sleeping for {e.value}s")
        await asleep(e.value)
        await _send_metadata_failed_reply(job, reason)
    except Exception as e:
        LOGGER.error(f"Failed to send metadata failure reply: {e}")


async def _metadata_for_job(job: dict) -> dict | None:
    if job.get("metadata_info"):
        return dict(job["metadata_info"])
    title = job.get("title") or job.get("file_name") or "unknown"
    return await metadata(
        clean_filename(title),
        int(job["channel"]),
        int(job["msg_id"]),
        override_id=job.get("override_id"),
        season_hint=job.get("season_hint"),
    )


async def _process_ingest_job(job: dict) -> None:
    title = job.get("title") or job.get("file_name") or "unknown"

    await _set_status(job, "processing metadata")
    await _set_status(job, "matching movie/series")
    metadata_info = await _metadata_for_job(job)
    if metadata_info is None:
        match_details = pop_match_failure(job.get("channel"), job.get("msg_id"))
        reason = match_details.get("match_rejection_reason") or "metadata_failed"
        if match_details:
            job["match_details"] = match_details
        await _finalize_failed_status_or_reply(job, reason)
        LOGGER.warning(f"Metadata failed for queued {job.get('source_type')} item: {title} (ID: {job.get('msg_id')})")
        return

    if job.get("source_type") == "telegram":
        finalized_name = finalize_media_name(
            job.get("display_name") or title,
            is_split=bool(metadata_info.get("group_key")),
        )
        job["display_name"] = finalized_name

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

    await _set_status(job, "indexing stream")
    updated_id = await db.insert_media(
        metadata_info,
        channel=int(job["channel"]),
        msg_id=int(job["msg_id"]),
        size=job.get("size") or "",
        name=job.get("display_name") or title,
        raw_size=int(job.get("file_size") or job.get("raw_size") or 0),
    )
    if updated_id:
        LOGGER.info(f"{metadata_info['media_type']} updated with ID: {updated_id}")
        announce_new_media(metadata_info)
        try:
            await mark_uploaded_for_media(metadata_info)
        except Exception as e:
            LOGGER.debug(f"Could not update request status for {metadata_info.get('title')}: {e}")
        await _set_status(job, "preparing reply")
        await reply_queue.put({
            "chat_id": job["chat_id"],
            "msg_id": job["original_msg_id"],
            "metadata_info": metadata_info,
            "title": job.get("display_name") or title,
            "size": job.get("size") or "",
            "status_chat_id": job.get("status_chat_id"),
            "status_msg_id": job.get("status_msg_id"),
        })
    else:
        reason = "database_insert_failed"
        if job.get("status_chat_id") and job.get("status_msg_id"):
            await _set_status(job, "failed", reason=reason, force=True)
        else:
            await _send_metadata_failed_reply(job, reason)
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
                        raw_size=0,
                    )
                    if updated_id:
                        LOGGER.info(f"{metadata_info['media_type']} updated with ID: {updated_id}")
                        announce_new_media(metadata_info)
                        try:
                            await mark_uploaded_for_media(metadata_info)
                        except Exception:
                            pass
                        await reply_queue.put({
                            "chat_id": chat_id,
                            "msg_id": original_msg_id,
                            "metadata_info": metadata_info,
                            "title": title,
                            "size": size,
                            "status_chat_id": None,
                            "status_msg_id": None,
                        })
                    else:
                        LOGGER.info("Update failed due to validation errors.")
        except Exception as e:
            LOGGER.exception(f"Ingestion worker failed: {e}")
        finally:
            file_queue.task_done()


async def send_reply_messages():
    from Backend.pyrofork.bot import StreamBot

    while True:
        reply_job = await reply_queue.get()
        if isinstance(reply_job, dict):
            chat_id = reply_job["chat_id"]
            msg_id = reply_job["msg_id"]
            metadata_info = reply_job["metadata_info"]
            title = reply_job.get("title") or ""
            size = reply_job.get("size") or ""
            status_chat_id = reply_job.get("status_chat_id")
            status_msg_id = reply_job.get("status_msg_id")
        else:
            chat_id, msg_id, metadata_info, title, size = reply_job
            status_chat_id = None
            status_msg_id = None
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
            detail_line = " | ".join(
                str(part)
                for part in (rating_str, quality, size)
                if part
            )
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
                        f"{detail_line}\n\n"
                        f"{help_note}"
                        f"{stream_note}"
                    )
                else:
                    reply_text = (
                        f"🎬 <b>{movie_title}</b>"
                        f"{f' ({year})' if year else ''}\n"
                        f"📺 S{season:02d}E{episode:02d}"
                        f"{f' - {ep_title}' if ep_title else ''}\n"
                        f"{detail_line}\n\n"
                        f"{help_note}"
                        f"{stream_note}"
                    )
            else:
                reply_text = (
                    f"🎬 <b>{movie_title}</b>"
                    f"{f' ({year})' if year else ''}\n"
                    f"{detail_line}\n\n"
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
            await _delete_status(status_chat_id, status_msg_id)
            LOGGER.info(f"Sent stream link reply for: {movie_title}")
        except FloodWait as e:
            LOGGER.info(f"FloodWait in reply: sleeping for {e.value}s")
            await asleep(e.value)
            await reply_queue.put(reply_job)
        except Exception as e:
            LOGGER.error(f"Failed to send reply message: {e}")
            await _edit_status(
                status_chat_id,
                status_msg_id,
                _format_queue_status("failed", title or "unknown", reason="final_reply_failed"),
            )
        reply_queue.task_done()


create_task(process_file())
create_task(send_reply_messages())


def _is_video_document(message: Message) -> bool:
    return bool(
        message.document and (
            (message.document.mime_type and message.document.mime_type.startswith("video/")) or
            (message.document.file_name and message.document.file_name.lower().endswith(VIDEO_EXTENSIONS)) or
            parse_split_info(message.caption or message.document.file_name or "")
        )
    )


def _manual_channel(chat_id: int) -> bool:
    normalized = str(chat_id).replace("-100", "")
    return normalized in {
        str(channel).strip().replace("-100", "")
        for channel in SettingsManager.current().manual_channels
    }


def _metadata_base_from_document(document: dict) -> dict:
    return {
        "tmdb_id": document.get("tmdb_id"),
        "imdb_id": document.get("imdb_id"),
        "title": document.get("title") or "",
        "year": document.get("release_year") or 0,
        "rate": document.get("rating") or 0,
        "description": document.get("description") or "",
        "poster": document.get("poster") or "",
        "backdrop": document.get("backdrop") or "",
        "logo": document.get("logo") or "",
        "genres": document.get("genres") or [],
        "cast": document.get("cast") or [],
        "runtime": str(document.get("runtime") or ""),
        "original_language": document.get("original_language"),
        "origin_country": document.get("origin_country") or [],
    }


async def _enqueue_personal_session(client: Client, message: Message, session: dict) -> None:
    resolved = await resolve_telegram_message(
        client,
        chat_id=str(message.chat.id).replace("-100", ""),
        msg_id=message.id,
    )
    document = await db.get_document(
        session["media_type"],
        int(session["tmdb_id"]),
        int(session["db_index"]),
    )
    if not document:
        raise ValueError("The active Manual Upload Session title no longer exists.")

    quality = resolved.get("quality") or session.get("quality") or "HD"
    split_key = resolved.get("split_key")
    metadata_info = _metadata_base_from_document(document)
    metadata_info.update({
        "media_type": session["media_type"],
        "quality": quality,
        "encoded_string": await encode_string({
            "chat_id": int(resolved["chat_id"]),
            "msg_id": int(resolved["msg_id"]),
        }),
        "group_key": f"manual:{resolved['chat_id']}:{quality}:{split_key}" if split_key else None,
        "part_number": resolved.get("part_number"),
        "is_anime": bool(document.get("is_anime")),
    })

    if session["media_type"] == "tv":
        season_number = int(session["season"])
        episode_number = await manual_session_manager.assign_episode(
            document,
            season_number,
            explicit_episode=session.get("episode"),
            split_key=split_key,
        )
        metadata_info.update({
            "season_number": season_number,
            "episode_number": episode_number,
            "episode_title": f"S{season_number:02d}E{episode_number:02d}",
            "episode_backdrop": metadata_info.get("backdrop") or "",
            "episode_overview": "",
            "episode_released": "",
        })

    await _enqueue_ingest_job(message, {
        "source_type": "telegram",
        "source_key": f"telegram:{message.chat.id}:{message.id}",
        "channel": int(resolved["chat_id"]),
        "chat_id": int(message.chat.id),
        "msg_id": int(message.id),
        "original_msg_id": int(message.id),
        "title": resolved["name"],
        "file_name": resolved["name"],
        "display_name": resolved["name"],
        "size": resolved["size"],
        "file_size": int(resolved.get("raw_size") or 0),
        "metadata_info": metadata_info,
        "message": message,
    })


def _is_torrent_document(message: Message) -> bool:
    return bool(
        message.document and (
            (message.document.file_name and message.document.file_name.lower().endswith(".torrent")) or
            message.document.mime_type == "application/x-bittorrent"
        )
    )


def _is_subtitle_document(message: Message) -> bool:
    return bool(message.document and is_subtitle_file(message.document.file_name or ""))


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

        message = callback_query.message
        target_chat_id = message.chat.id if message else int(requester["user_id"])
        reply_to_message_id = message.id if message else None
        await client.send_message(
            chat_id=target_chat_id,
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
            reply_to_message_id=reply_to_message_id,
        )
        await db.mark_watch_link_delivery(request_id, "sent")
        await callback_query.answer("Watch link posted in the channel.", show_alert=False)
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

    await _enqueue_ingest_job(message, {
        "source_type": "torrent",
        "source_key": f"torrent:{message.chat.id}:{msg_id}:{item.info_hash}:{item.file_idx}",
        "item_key": f"{item.info_hash}:{item.file_idx}",
        "channel": channel,
        "chat_id": int(message.chat.id),
        "msg_id": msg_id,
        "original_msg_id": message.id,
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
        LOGGER.warning(f"No supported torrent video items found in message {message.id}.")
        return True

    LOGGER.info(f"Queued {queued} torrent stream(s) from message {message.id}.")
    return True


@Client.on_message(filters.channel & (filters.document | filters.video | filters.text))
async def file_receive_handler(client: Client, message: Message):
    is_manual = _manual_channel(message.chat.id)
    if str(message.chat.id) in Telegram.AUTH_CHANNEL or is_manual:
        try:
            if _is_subtitle_document(message):
                name = message.document.file_name or message.caption or "subtitle.srt"
                channel = int(str(message.chat.id).replace("-100", ""))
                ok = await ingest_subtitle(name, channel, int(message.id), caption=message.caption)
                await message.reply_text(
                    "✅ Subtitle indexed." if ok else "⚠️ Subtitle metadata match failed. Add IMDb/TMDb link in the caption or use a clearer subtitle filename.",
                    quote=True,
                    disable_web_page_preview=True,
                )
                return

            if await _handle_torrent_message(client, message):
                return

            is_video_doc = _is_video_document(message)
            if message.video or is_video_doc:
                manual_session = manual_session_manager.current() if is_manual else None
                if is_manual and not manual_session:
                    await message.reply_text(
                        "No Manual Upload Session is active. Start one from Admin → Tools, then resend the file.",
                        quote=True,
                    )
                    return
                if manual_session and manual_session.get("kind") == "personal":
                    await _enqueue_personal_session(client, message, manual_session)
                    return

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

                title = finalize_media_name(title)

                if Backend.USE_DEFAULT_ID:
                    new_caption = (message.caption + "\n\n" + Backend.USE_DEFAULT_ID) if message.caption else Backend.USE_DEFAULT_ID
                    create_task(edit_message(chat_id=message.chat.id, msg_id=message.id, new_caption=new_caption))

                await _enqueue_ingest_job(message, {
                    "source_type": "telegram",
                    "source_key": f"telegram:{message.chat.id}:{msg_id}",
                    "channel": int(channel),
                    "chat_id": int(message.chat.id),
                    "msg_id": int(msg_id),
                    "original_msg_id": message.id,
                    "title": title,
                    "file_name": file_name or title,
                    "display_name": title,
                    "size": size,
                    "file_size": int(getattr(file, "file_size", 0) or 0),
                    "override_id": (
                        manual_session.get("default_id")
                        if manual_session and manual_session.get("kind") == "real"
                        else extract_default_id(message.caption) if message.caption else None
                    ),
                    "season_hint": manual_session.get("season") if manual_session else None,
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
            if _is_subtitle_document(message):
                name = message.document.file_name or message.caption or "subtitle.srt"
                channel = int(str(message.chat.id).replace("-100", ""))
                ok = await ingest_subtitle(name, channel, int(message.id), caption=message.caption)
                if ok:
                    await message.reply_text("✅ Subtitle updated.", quote=True, disable_web_page_preview=True)
                return

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
                    title = finalize_media_name(title)
                    await _enqueue_ingest_job(message, {
                        "source_type": "telegram",
                        "source_key": f"telegram:{message.chat.id}:{msg_id}",
                        "channel": int(channel),
                        "chat_id": int(message.chat.id),
                        "msg_id": int(msg_id),
                        "original_msg_id": message.id,
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
                    sub_deleted = await remove_subtitle(int(message.chat.id), int(msg_id))
                    if deleted:
                        LOGGER.info(f"Automatically purged deleted message {msg_id} from database.")
                    if sub_deleted:
                        LOGGER.info(f"Automatically purged deleted subtitle {msg_id} from database.")
                except Exception as ex:
                    LOGGER.error(f"Failed to scrub deleted message {msg_id}: {ex}")
    except Exception as e:
        LOGGER.error(f"Error handling deleted messages: {e}")
