import asyncio
import time
import secrets
from hashlib import sha256
from collections import deque
from typing import Any, Callable, Dict, List, Union, Optional, Tuple
import traceback
from fastapi import Request
from pyrogram import Client, raw, utils
from pyrogram.crypto import aes
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Session, Auth
from Backend.logger import LOGGER
from Backend.helper.exceptions import FIleNotFound
from Backend.helper.pyro import get_file_ids
from Backend import db
from Backend.pyrofork.bot import (
    work_loads,
    multi_clients,
    client_dc_map,
    client_failures,
    client_avg_mbps,
    client_cooldowns,
    client_dc_cooldowns,
    client_last_errors,
)
from Backend.config import Telegram

ACTIVE_STREAMS: Dict[str, Dict] = {}
RECENT_STREAMS = deque(maxlen=20)
client_dc_avg_mbps: Dict[Tuple[int, int], float] = {}
client_dc_ttfb_sec: Dict[Tuple[int, int], float] = {}


class TelegramCdnFetchError(RuntimeError):
    """Raised when a Telegram CDN redirect cannot be fetched safely."""


def _ema(previous: float, current: float, weight: float = 0.3) -> float:
    if previous <= 0:
        return current
    return (1 - weight) * previous + weight * current


def update_client_dc_metrics(
    client_index: int,
    target_dc: int,
    mbps: float,
    ttfb_sec: Optional[float] = None,
) -> None:
    """Remember how a helper performs against the file's Telegram DC."""
    try:
        target_dc = int(target_dc or 0)
        mbps = float(mbps or 0.0)
        if target_dc <= 0 or mbps <= 0:
            return

        key = (int(client_index), target_dc)
        client_dc_avg_mbps[key] = _ema(float(client_dc_avg_mbps.get(key, 0.0) or 0.0), mbps)

        if ttfb_sec is not None:
            ttfb_sec = float(ttfb_sec)
            if ttfb_sec > 0:
                client_dc_ttfb_sec[key] = _ema(float(client_dc_ttfb_sec.get(key, 0.0) or 0.0), ttfb_sec)
    except Exception:
        pass


def _cooldown_until(value) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def is_client_cooled_down(client_index: int, target_dc: int = 0, now: Optional[float] = None) -> bool:
    now = time.time() if now is None else now
    if _cooldown_until(client_cooldowns.get(int(client_index))) > now:
        return True
    if target_dc and _cooldown_until(client_dc_cooldowns.get((int(client_index), int(target_dc)))) > now:
        return True
    return False


def get_client_cooldown_state(now: Optional[float] = None) -> dict:
    now = time.time() if now is None else now
    states = {}
    for idx in set(list(client_cooldowns.keys()) + [k[0] for k in client_dc_cooldowns.keys()]):
        global_until = _cooldown_until(client_cooldowns.get(idx))
        dc_states = {
            str(dc): max(0, round(_cooldown_until(until) - now, 1))
            for (client_idx, dc), until in client_dc_cooldowns.items()
            if client_idx == idx and _cooldown_until(until) > now
        }
        states[str(idx)] = {
            "global_sec": max(0, round(global_until - now, 1)),
            "dc": dc_states,
            "last_error": client_last_errors.get(idx),
        }
    return states


def record_route_failure(
    client_index: int,
    target_dc: int,
    reason: str,
    *,
    stream_id: Optional[str] = None,
    offset: Optional[int] = None,
    attempt: Optional[int] = None,
) -> None:
    client_index = int(client_index)
    target_dc = int(target_dc or 0)
    client_failures[client_index] = client_failures.get(client_index, 0) + 1
    client_last_errors[client_index] = {
        "reason": str(reason)[:240],
        "target_dc": target_dc,
        "stream_id": stream_id,
        "offset": offset,
        "attempt": attempt,
        "ts": time.time(),
    }

    threshold = max(1, int(getattr(Telegram, "SMART_ROUTING_COOLDOWN_FAILURES", 2) or 2))
    if client_failures.get(client_index, 0) >= threshold:
        until = time.time() + max(30, int(getattr(Telegram, "SMART_ROUTING_COOLDOWN_SEC", 180) or 180))
        client_cooldowns[client_index] = until
        if target_dc:
            client_dc_cooldowns[(client_index, target_dc)] = until
        LOGGER.warning(
            "Client cooldown: client=%s target_dc=%s reason=%s failures=%s until=%s",
            client_index,
            target_dc,
            reason,
            client_failures.get(client_index),
            int(until),
        )


def smart_client_score(client_index: int, target_dc: int):
    """Lower score wins. Uses exact DC, live failures, and per-file-DC speed."""
    target_dc = int(target_dc or 0)
    cooldown_penalty = 1 if is_client_cooled_down(client_index, target_dc) else 0
    same_dc_penalty = 0 if target_dc and client_dc_map.get(client_index) == target_dc else 1
    failure_load = work_loads.get(client_index, 0) + 3 * client_failures.get(client_index, 0)
    dc_speed = float(client_dc_avg_mbps.get((client_index, target_dc), 0.0) or 0.0)
    dc_ttfb = float(client_dc_ttfb_sec.get((client_index, target_dc), 0.0) or 0.0)
    global_speed = float(client_avg_mbps.get(client_index, 0.0) or 0.0)

    return (
        cooldown_penalty,
        same_dc_penalty,
        failure_load,
        dc_ttfb if dc_ttfb > 0 else 999.0,
        -dc_speed,
        -global_speed,
        client_index,
    )


def get_adaptive_chunk_size(client_index: int) -> int:
    """Return the best chunk size (bytes) for this client based on recent speed.

    Speed tiers:
      < 5  MB/s  → 512 KB  (small chunks, faster first-byte on slow sessions)
      5-20 MB/s  →   1 MB  (default)
      20-60 MB/s →   2 MB  (fewer round-trips on fast sessions)
      > 60 MB/s  →   4 MB  (maximise throughput on very fast sessions)
    """
    speed = client_avg_mbps.get(client_index, 0.0)
    if speed >= 60:
        return 4 * 1024 * 1024
    if speed >= 20:
        return 2 * 1024 * 1024
    if speed >= 5:
        return 1 * 1024 * 1024
    # Unknown speed or < 5 MB/s → start conservative
    return 512 * 1024

class ByteStreamer:
    CHUNK_SIZE = 1024 * 1024  # 1 MB
    CLEAN_INTERVAL = 30 * 60  # 30 minutes
    _instances: Dict[int, "ByteStreamer"] = {}  # client_index → streamer (for fallback)

    def __init__(self, client: Client, client_index: int = -1):
        self.client = client
        self.client_index = client_index
        self._file_id_cache: Dict[Tuple[int, int], FileId] = {}
        self._session_lock = asyncio.Lock()
        self._cdn_session_lock = asyncio.Lock()
        self._cdn_sessions: Dict[int, Session] = {}
        self._cdn_getfile_supported = True
        # Register this streamer so fallback logic can reuse it
        if client_index >= 0:
            ByteStreamer._instances[client_index] = self
        asyncio.create_task(self._clean_cache())
        asyncio.create_task(self._prewarm_sessions())

    async def _prewarm_sessions(self):
        common_dcs = [1, 2, 4, 5]  # Main Telegram DCs
        LOGGER.debug("Pre-warming media sessions for common DCs...")
        
        for dc in common_dcs:
            try:
                if dc in self.client.media_sessions:
                    LOGGER.debug(f"Media session for DC {dc} already exists, skipping")
                    continue

                test_mode = await self.client.storage.test_mode()
                current_dc = await self.client.storage.dc_id()
 
                if dc == current_dc:
                    continue
                
                auth_key = await Auth(self.client, dc, test_mode).create()
                session = Session(self.client, dc, auth_key, test_mode, is_media=True)
                session.no_updates = True
                session.timeout = 30
                session.sleep_threshold = 60
                
                await session.start()
                
                for attempt in range(6):
                    try:
                        exported = await self.client.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=dc)
                        )
                        await session.send(
                            raw.functions.auth.ImportAuthorization(
                                id=exported.id, bytes=exported.bytes
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        LOGGER.debug(f"AuthBytesInvalid during pre-warm for DC {dc}; retrying...")
                        await asyncio.sleep(0.5)
                    except OSError:
                        LOGGER.debug(f"OSError during pre-warm for DC {dc}; retrying...")
                        await asyncio.sleep(1)
                    except Exception as e:
                        LOGGER.debug(f"Error during pre-warm for DC {dc}: {e}")
                        break
                
                self.client.media_sessions[dc] = session
                LOGGER.debug(f"Pre-warmed media session for DC {dc}")
                
            except Exception as e:
                LOGGER.debug(f"Could not pre-warm DC {dc}: {e}")
                continue

    async def get_file_properties(self, chat_id: int, message_id: int) -> FileId:
        cache_key = (int(chat_id), int(message_id))
        if cache_key not in self._file_id_cache:
            file_id = await get_file_ids(self.client, int(chat_id), int(message_id))
            if not file_id:
                LOGGER.warning("Message %s not found", message_id)
                raise FIleNotFound
            self._file_id_cache[cache_key] = file_id
        return self._file_id_cache[cache_key]

    async def _send_upload_get_file(
        self,
        media_session: Session,
        location: Union[
            raw.types.InputPhotoFileLocation,
            raw.types.InputDocumentFileLocation,
            raw.types.InputPeerPhotoFileLocation,
        ],
        offset: int,
        limit: int,
    ) -> Any:
        if bool(getattr(Telegram, "TELEGRAM_CDN_ENABLED", True)) and self._cdn_getfile_supported:
            try:
                request = raw.functions.upload.GetFile(
                    location=location,
                    offset=offset,
                    limit=limit,
                    cdn_supported=True,
                )
            except TypeError:
                self._cdn_getfile_supported = False
                LOGGER.warning("Telegram CDN disabled for this streamer: PyroFork GetFile has no cdn_supported flag")
            else:
                return await media_session.send(request)

        return await media_session.send(
            raw.functions.upload.GetFile(location=location, offset=offset, limit=limit)
        )

    async def _fetch_file_bytes(
        self,
        media_session: Session,
        location: Union[
            raw.types.InputPhotoFileLocation,
            raw.types.InputDocumentFileLocation,
            raw.types.InputPeerPhotoFileLocation,
        ],
        offset: int,
        limit: int,
        route_event: Optional[Callable[[dict], None]] = None,
        stream_stats: Optional[dict] = None,
    ) -> Optional[bytes]:
        response = await self._send_upload_get_file(media_session, location, offset, limit)
        if isinstance(response, raw.types.upload.FileCdnRedirect):
            return await self._fetch_cdn_redirect(
                origin_session=media_session,
                redirect=response,
                offset=offset,
                limit=limit,
                route_event=route_event,
                stream_stats=stream_stats,
            )
        return getattr(response, "bytes", None) if response else None

    async def _get_cdn_session(self, dc_id: int) -> Session:
        dc_id = int(dc_id)
        cdn_session = self._cdn_sessions.get(dc_id)
        if cdn_session:
            return cdn_session

        async with self._cdn_session_lock:
            cdn_session = self._cdn_sessions.get(dc_id)
            if cdn_session:
                return cdn_session

            test_mode = await self.client.storage.test_mode()
            auth_key = await Auth(self.client, dc_id, test_mode).create()
            cdn_session = Session(self.client, dc_id, auth_key, test_mode, is_media=True, is_cdn=True)
            cdn_session.no_updates = True
            cdn_session.timeout = 30
            cdn_session.sleep_threshold = 60
            await cdn_session.start()
            self._cdn_sessions[dc_id] = cdn_session
            LOGGER.debug("Created Telegram CDN session for DC %s", dc_id)
            return cdn_session

    async def _fetch_cdn_redirect(
        self,
        origin_session: Session,
        redirect: raw.types.upload.FileCdnRedirect,
        offset: int,
        limit: int,
        route_event: Optional[Callable[[dict], None]] = None,
        stream_stats: Optional[dict] = None,
    ) -> bytes:
        cdn_dc = int(getattr(redirect, "dc_id", 0) or 0)
        self._record_cdn_stat(stream_stats, "cdn_redirects", 1)
        self._record_cdn_stat(stream_stats, "cdn_dc", cdn_dc, replace=True)
        self._emit_cdn_event(
            route_event,
            {
                "event": "cdn_redirect",
                "offset": offset,
                "limit": limit,
                "cdn_dc": cdn_dc,
            },
        )

        cdn_session = await self._get_cdn_session(cdn_dc)
        max_reuploads = max(0, int(getattr(Telegram, "TELEGRAM_CDN_MAX_REUPLOAD_ATTEMPTS", 2) or 2))
        reuploads = 0

        while True:
            response = await cdn_session.send(
                raw.functions.upload.GetCdnFile(
                    file_token=redirect.file_token,
                    offset=offset,
                    limit=limit,
                )
            )

            if isinstance(response, raw.types.upload.CdnFileReuploadNeeded):
                self._emit_cdn_event(
                    route_event,
                    {
                        "event": "cdn_reupload_needed",
                        "offset": offset,
                        "limit": limit,
                        "cdn_dc": cdn_dc,
                        "attempt": reuploads + 1,
                    },
                )
                if reuploads >= max_reuploads:
                    self._record_cdn_stat(stream_stats, "cdn_errors", 1)
                    raise TelegramCdnFetchError(f"cdn_reupload_exhausted dc={cdn_dc} offset={offset}")
                await origin_session.send(
                    raw.functions.upload.ReuploadCdnFile(
                        file_token=redirect.file_token,
                        request_token=response.request_token,
                    )
                )
                reuploads += 1
                continue

            encrypted = getattr(response, "bytes", None) if response else None
            if encrypted is None:
                self._record_cdn_stat(stream_stats, "cdn_errors", 1)
                raise TelegramCdnFetchError(f"empty_cdn_response dc={cdn_dc} offset={offset}")

            decrypted = self._decrypt_cdn_bytes(
                encrypted=encrypted,
                key=redirect.encryption_key,
                iv=redirect.encryption_iv,
                offset=offset,
            )
            if bool(getattr(Telegram, "TELEGRAM_CDN_VERIFY_HASHES", True)):
                await self._verify_cdn_hashes(
                    origin_session=origin_session,
                    redirect=redirect,
                    offset=offset,
                    data=decrypted,
                    route_event=route_event,
                    stream_stats=stream_stats,
                )

            self._record_cdn_stat(stream_stats, "cdn_chunks", 1)
            self._record_cdn_stat(stream_stats, "cdn_bytes", len(decrypted))
            self._emit_cdn_event(
                route_event,
                {
                    "event": "cdn_fetch",
                    "offset": offset,
                    "limit": limit,
                    "bytes": len(decrypted),
                    "cdn_dc": cdn_dc,
                },
            )
            if bool(getattr(Telegram, "TELEGRAM_CDN_DEBUG_LOGS", False)):
                LOGGER.debug("Telegram CDN fetch dc=%s offset=%s limit=%s bytes=%s", cdn_dc, offset, limit, len(decrypted))
            return decrypted

    @staticmethod
    def _decrypt_cdn_bytes(encrypted: bytes, key: bytes, iv: bytes, offset: int) -> bytes:
        iv_bytes = bytes(iv)
        if len(iv_bytes) < 4:
            raise TelegramCdnFetchError("invalid_cdn_iv")
        ctr_iv = bytearray(iv_bytes[:-4] + (max(0, int(offset)) // 16).to_bytes(4, "big"))
        return aes.ctr256_decrypt(encrypted, key, ctr_iv)

    async def _verify_cdn_hashes(
        self,
        origin_session: Session,
        redirect: raw.types.upload.FileCdnRedirect,
        offset: int,
        data: bytes,
        route_event: Optional[Callable[[dict], None]] = None,
        stream_stats: Optional[dict] = None,
    ) -> None:
        hashes = list(getattr(redirect, "file_hashes", None) or [])
        if not self._has_applicable_cdn_hash(hashes, offset, len(data)):
            hashes = await origin_session.send(
                raw.functions.upload.GetCdnFileHashes(
                    file_token=redirect.file_token,
                    offset=offset,
                )
            )

        for file_hash in list(hashes or []):
            hash_offset = int(getattr(file_hash, "offset", -1))
            hash_limit = int(getattr(file_hash, "limit", 0) or 0)
            if hash_limit <= 0:
                continue
            start = hash_offset - offset
            end = start + hash_limit
            if start < 0 or end > len(data):
                continue
            if sha256(data[start:end]).digest() != getattr(file_hash, "hash", b""):
                self._record_cdn_stat(stream_stats, "cdn_errors", 1)
                self._emit_cdn_event(
                    route_event,
                    {
                        "event": "cdn_hash_failed",
                        "offset": offset,
                        "hash_offset": hash_offset,
                        "hash_limit": hash_limit,
                        "cdn_dc": int(getattr(redirect, "dc_id", 0) or 0),
                    },
                )
                raise TelegramCdnFetchError(f"cdn_hash_mismatch offset={offset} hash_offset={hash_offset}")

    @staticmethod
    def _has_applicable_cdn_hash(hashes: list, offset: int, data_len: int) -> bool:
        for file_hash in hashes:
            hash_offset = int(getattr(file_hash, "offset", -1))
            hash_limit = int(getattr(file_hash, "limit", 0) or 0)
            start = hash_offset - offset
            if hash_limit > 0 and start >= 0 and start + hash_limit <= data_len:
                return True
        return False

    @staticmethod
    def _record_cdn_stat(stream_stats: Optional[dict], key: str, value: int, replace: bool = False) -> None:
        if stream_stats is None:
            return
        try:
            if replace:
                stream_stats[key] = value
            else:
                stream_stats[key] = int(stream_stats.get(key, 0) or 0) + int(value or 0)
        except Exception:
            pass

    @staticmethod
    def _emit_cdn_event(route_event: Optional[Callable[[dict], None]], event: dict) -> None:
        if not route_event:
            return
        try:
            route_event(event)
        except Exception:
            pass

    async def prefetch_stream(
        self,
        file_id: FileId,
        client_index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
        prefetch: int = 3,
        stream_id: Optional[str] = None,
        meta: Optional[dict] = None,
        parallelism: int = 2,
        request: Optional[Request] = None,
        chat_id: Optional[int] = None,
        message_id: Optional[int] = None,
    ):
        if not stream_id:
            stream_id = secrets.token_hex(8)

        now = time.time()
        registry_entry = {
            "stream_id": stream_id,
            "msg_id": getattr(file_id, "local_id", None) or None,
            "chat_id": getattr(file_id, "chat_id", None),
            "dc_id": file_id.dc_id,
            "client_index": client_index,
            "start_ts": now,
            "last_ts": now,
            "ttfb_sec": None,
            "total_bytes": 0,
            "avg_mbps": 0.0,
            "instant_mbps": 0.0,
            "peak_mbps": 0.0,
            "recent_measurements": deque(maxlen=3),
            "status": "active",
            "part_count": part_count,
            "prefetch": prefetch,
            "meta": meta or {},
            "chunk_timeouts": 0,
            "chunk_errors": 0,
            "fallback_chunks": 0,
            "zero_pad_chunks": 0,
            "cdn_redirects": 0,
            "cdn_chunks": 0,
            "cdn_bytes": 0,
            "cdn_errors": 0,
            "cdn_dc": None,
            "error_reason": None,
            "route_attempts": [],
        }

        ACTIVE_STREAMS[stream_id] = registry_entry
        work_loads[client_index] += 1

        queue_maxsize = max(1, prefetch)
        q: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        stop_event = asyncio.Event()

        media_session = await self._get_media_session(file_id)
        location = await self._get_location(file_id)
        target_dc = int(getattr(file_id, "dc_id", 0) or 0)

        def remember_route_attempt(event: dict) -> None:
            try:
                attempts = registry_entry.setdefault("route_attempts", [])
                event["ts"] = round(time.time(), 3)
                attempts.append(event)
                if len(attempts) > 30:
                    del attempts[:-30]
            except Exception:
                pass

        async def fetch_chunk_with_retries(seq_idx: int, off: int) -> Tuple[int, Optional[bytes], Optional[str]]:
            """Fetch one chunk with timeout, exponential back-off, and bot fallback.

            Retry schedule (max 3 tries):
              tries 0    → same bot / same session, 15 s timeout each
              tries 1-2  → try a healthier fallback bot (if available),
                           still with 15 s timeout
            On every TimeoutError the primary client's failure counter is incremented
            so select_best_client will avoid it for future requests.
            """
            tries = 0
            while tries < 3 and not stop_event.is_set():
                # --- choose which media session to use this attempt ---
                use_session = media_session
                use_location = location
                use_client_idx = client_index
                if tries >= 1 and len(multi_clients) > 1:
                    # Pick the best other helper for this file's Telegram DC.
                    fallback_pool = [
                        i for i in multi_clients
                        if i != client_index and not is_client_cooled_down(i, target_dc)
                    ]
                    fallback_idx = min(
                        fallback_pool,
                        key=lambda idx: smart_client_score(idx, target_dc),
                        default=None,
                    )
                    if fallback_idx is not None:
                        fb_streamer = ByteStreamer._instances.get(fallback_idx)
                        if fb_streamer is None:
                            fb_streamer = ByteStreamer(multi_clients[fallback_idx], fallback_idx)
                        try:
                            fallback_file_id = file_id
                            if chat_id is not None and message_id is not None:
                                fallback_file_id = await fb_streamer.get_file_properties(
                                    chat_id=int(chat_id),
                                    message_id=int(message_id),
                                )
                            use_session = await fb_streamer._get_media_session(fallback_file_id)
                            use_location = await fb_streamer._get_location(fallback_file_id)
                            use_client_idx = fallback_idx
                            LOGGER.debug(
                                "Chunk fallback: seq=%s try=%s primary=%s → fallback=%s",
                                seq_idx, tries, client_index, fallback_idx,
                            )
                            registry_entry["fallback_chunks"] = registry_entry.get("fallback_chunks", 0) + 1
                            remember_route_attempt({
                                "event": "fallback_selected",
                                "seq": seq_idx,
                                "offset": off,
                                "attempt": tries + 1,
                                "primary_client": client_index,
                                "client": fallback_idx,
                                "target_dc": target_dc,
                            })
                        except Exception:
                            use_session = media_session  # revert if fallback session fails
                            use_location = location
                            use_client_idx = client_index

                # --- attempt the fetch with a hard timeout ---
                try:
                    base_timeout = float(getattr(Telegram, "SMART_ROUTING_CHUNK_TIMEOUT_SEC", 15.0) or 15.0)
                    first_timeout = float(getattr(Telegram, "SMART_ROUTING_FIRST_CHUNK_TIMEOUT_SEC", 4.0) or 4.0)
                    timeout = base_timeout
                    if seq_idx == 0 and tries == 0 and len(multi_clients) > 1:
                        timeout = max(1.5, min(first_timeout, base_timeout))

                    fetch_started = time.perf_counter()
                    remember_route_attempt({
                        "event": "chunk_attempt",
                        "seq": seq_idx,
                        "offset": off,
                        "attempt": tries + 1,
                        "client": use_client_idx,
                        "target_dc": target_dc,
                    })
                    chunk_bytes = await asyncio.wait_for(
                        self._fetch_file_bytes(
                            media_session=use_session,
                            location=use_location,
                            offset=off,
                            limit=chunk_size,
                            route_event=remember_route_attempt,
                            stream_stats=registry_entry,
                        ),
                        timeout=timeout,
                    )
                    elapsed = max(time.perf_counter() - fetch_started, 1e-6)
                    
                    if chunk_bytes == b"":
                        reason = f"empty_chunk client={use_client_idx} seq={seq_idx} offset={off}"
                        remember_route_attempt({
                            "event": "chunk_empty",
                            "seq": seq_idx,
                            "offset": off,
                            "attempt": tries + 1,
                            "client": use_client_idx,
                            "target_dc": target_dc,
                            "reason": reason,
                        })
                        return seq_idx, None, reason

                    if chunk_bytes:
                        mbps = (len(chunk_bytes) / (1024 * 1024)) / elapsed
                        update_client_dc_metrics(use_client_idx, target_dc, mbps, elapsed)

                    # If we succeeded via a fallback, mark primary as degraded
                    if use_client_idx != client_index:
                        record_route_failure(
                            client_index,
                            target_dc,
                            "fallback_needed",
                            stream_id=stream_id,
                            offset=off,
                            attempt=tries + 1,
                        )
                    return seq_idx, chunk_bytes, None

                except asyncio.TimeoutError:
                    tries += 1
                    record_route_failure(
                        use_client_idx,
                        target_dc,
                        "chunk_timeout",
                        stream_id=stream_id,
                        offset=off,
                        attempt=tries,
                    )
                    registry_entry["chunk_timeouts"] = registry_entry.get("chunk_timeouts", 0) + 1
                    LOGGER.warning(
                        "Chunk timeout stream=%s seq=%s off=%s try=%s client=%s target_dc=%s",
                        stream_id, seq_idx, off, tries, use_client_idx, target_dc,
                    )
                    remember_route_attempt({
                        "event": "chunk_timeout",
                        "seq": seq_idx,
                        "offset": off,
                        "attempt": tries,
                        "client": use_client_idx,
                        "target_dc": target_dc,
                    })
                except Exception as e:
                    tries += 1
                    record_route_failure(
                        use_client_idx,
                        target_dc,
                        f"chunk_error:{type(e).__name__}",
                        stream_id=stream_id,
                        offset=off,
                        attempt=tries,
                    )
                    registry_entry["chunk_errors"] = registry_entry.get("chunk_errors", 0) + 1
                    LOGGER.debug(
                        "Fetch chunk error stream=%s seq=%s off=%s try=%s client=%s target_dc=%s err=%s",
                        stream_id, seq_idx, off, tries, use_client_idx, target_dc, getattr(e, "args", e),
                    )
                    remember_route_attempt({
                        "event": "chunk_error",
                        "seq": seq_idx,
                        "offset": off,
                        "attempt": tries,
                        "client": use_client_idx,
                        "target_dc": target_dc,
                        "error": str(e)[:240],
                    })

                # Exponential back-off: 0.5 s, 1 s, 2 s, 4 s, 8 s, 10 s (cap)
                await asyncio.sleep(min(0.5 * (2 ** (tries - 1)), 10.0))

            reason = f"failed_after_retries seq={seq_idx} offset={off} client={client_index} target_dc={target_dc}"
            registry_entry["status"] = "error"
            registry_entry["error_reason"] = reason
            LOGGER.error("Stream chunk failure: stream=%s %s", stream_id, reason)
            return seq_idx, None, reason

        async def producer():
            scheduled_tasks: Dict[int, asyncio.Task] = {}
            try:
                if part_count <= 0:
                    await q.put((None, None))
                    return

                next_to_schedule = 0
                results_buffer = {}
                next_to_put = 0
                max_parallel = max(1, parallelism)

                initial = min(part_count, max_parallel)
                for i in range(initial):
                    seq = next_to_schedule
                    off = offset + seq * chunk_size
                    task = asyncio.create_task(fetch_chunk_with_retries(seq, off))
                    scheduled_tasks[seq] = task
                    next_to_schedule += 1

                while next_to_put < part_count:
                    if stop_event.is_set():
                        break

                    if not scheduled_tasks:
                        seq = next_to_schedule
                        off = offset + seq * chunk_size
                        task = asyncio.create_task(fetch_chunk_with_retries(seq, off))
                        scheduled_tasks[seq] = task
                        next_to_schedule += 1

                    done, _ = await asyncio.wait(scheduled_tasks.values(), return_when=asyncio.FIRST_COMPLETED)

                    for completed in done:
                        try:
                            completed_seq = None
                            for k, t in list(scheduled_tasks.items()):
                                if t is completed:
                                    completed_seq = k
                                    break

                            if completed_seq is None:
                                continue

                            seq_idx, chunk_bytes, error_reason = completed.result()
                            scheduled_tasks.pop(completed_seq, None)

                            if chunk_bytes is None:
                                registry_entry["status"] = "error"
                                registry_entry["error_reason"] = error_reason or f"chunk_unavailable seq={seq_idx}"
                                LOGGER.error(
                                    "Chunk unavailable for stream=%s seq=%s reason=%s. Ending stream cleanly.",
                                    stream_id,
                                    seq_idx,
                                    registry_entry["error_reason"],
                                )
                                await q.put((None, None))
                                return

                            results_buffer[seq_idx] = chunk_bytes

                            if next_to_schedule < part_count:
                                seq = next_to_schedule
                                off = offset + seq * chunk_size
                                task = asyncio.create_task(fetch_chunk_with_retries(seq, off))
                                scheduled_tasks[seq] = task
                                next_to_schedule += 1

                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            LOGGER.exception("Error processing completed fetch task: %s%s", e, traceback.format_exc())
                            await q.put((None, None))
                            return

                    while next_to_put in results_buffer:
                        chunk_bytes = results_buffer.pop(next_to_put)
                        await q.put((offset + next_to_put * chunk_size, chunk_bytes))
                        next_to_put += 1

                await q.put((None, None))

            except asyncio.CancelledError:
                LOGGER.debug("Producer cancelled for stream %s", stream_id)
                try:
                    await q.put((None, None))
                except Exception:
                    pass
                raise
            except Exception as e:
                LOGGER.exception("Producer unexpected error for stream %s: %s", stream_id, e)
                registry_entry["status"] = "error"
                registry_entry["error_reason"] = f"producer_error:{type(e).__name__}"
                try:
                    await q.put((None, None))
                except Exception:
                    pass
            finally:
                # Ensure any in-flight chunk fetch tasks are stopped when the stream ends.
                for t in list(scheduled_tasks.values()):
                    try:
                        if not t.done():
                            t.cancel()
                    except Exception:
                        pass
                if scheduled_tasks:
                    try:
                        await asyncio.gather(*scheduled_tasks.values(), return_exceptions=True)
                    except Exception:
                        pass

        async def consumer_generator():
            producer_task = asyncio.create_task(producer())
            current_part_idx = 1
            first_yielded = False
            finalized = False

            try:
                while True:
                    try:
                        if request and await request.is_disconnected():
                            LOGGER.debug("Client disconnected for stream %s; cancelling stream", stream_id)
                            registry_entry["status"] = "cancelled"
                            stop_event.set()
                            break
                    except Exception:
                        pass

                    off_chunk = await q.get()
                    if off_chunk is None:
                        break

                    off, chunk = off_chunk
                    if off is None and chunk is None:
                        break

                    try:
                        chunk_len = len(chunk)
                    except Exception:
                        chunk_len = 0

                    now_ts = time.time()
                    elapsed = now_ts - registry_entry["last_ts"]
                    if elapsed <= 0:
                        elapsed = 1e-6

                    recent = registry_entry["recent_measurements"]
                    recent.append((chunk_len, elapsed))

                    if len(recent) >= 2:
                        total_bytes = sum(b for b, _ in recent)
                        total_time = sum(t for _, t in recent)
                        instant_mbps = min((total_bytes / (1024 * 1024)) / max(total_time, 0.01), 1000.0)
                    else:
                        instant_mbps = 0.0

                    registry_entry["total_bytes"] += chunk_len
                    registry_entry["last_ts"] = now_ts

                    total_time = now_ts - registry_entry["start_ts"]
                    if total_time <= 0:
                        total_time = 1e-6

                    registry_entry["avg_mbps"] = (registry_entry["total_bytes"] / (1024 * 1024)) / total_time
                    registry_entry["instant_mbps"] = instant_mbps

                    if instant_mbps > registry_entry["peak_mbps"]:
                        registry_entry["peak_mbps"] = instant_mbps

                    if part_count == 1:
                        if not first_yielded:
                            registry_entry["ttfb_sec"] = round(time.time() - registry_entry["start_ts"], 3)
                            first_yielded = True
                        yield chunk[first_part_cut:last_part_cut]
                    elif current_part_idx == 1:
                        if not first_yielded:
                            registry_entry["ttfb_sec"] = round(time.time() - registry_entry["start_ts"], 3)
                            first_yielded = True
                        yield chunk[first_part_cut:]
                    elif current_part_idx == part_count:
                        if not first_yielded:
                            registry_entry["ttfb_sec"] = round(time.time() - registry_entry["start_ts"], 3)
                            first_yielded = True
                        yield chunk[:last_part_cut]
                    else:
                        if not first_yielded:
                            registry_entry["ttfb_sec"] = round(time.time() - registry_entry["start_ts"], 3)
                            first_yielded = True
                        yield chunk

                    current_part_idx += 1

            except asyncio.CancelledError:
                LOGGER.debug("Consumer cancelled for stream %s", stream_id)
                if not producer_task.done():
                    producer_task.cancel()
                registry_entry["status"] = "cancelled"
                raise
            except Exception as e:
                LOGGER.exception("Consumer error for stream %s: %s", stream_id, e)
                registry_entry["status"] = "error"
                registry_entry["error_reason"] = f"consumer_error:{type(e).__name__}"
                if not producer_task.done():
                    producer_task.cancel()
                raise
            finally:
                if finalized:
                    return
                finalized = True

                if not producer_task.done():
                    try:
                        producer_task.cancel()
                        await asyncio.wait_for(producer_task, timeout=2.0)
                    except (Exception, asyncio.CancelledError):
                        pass

                try:
                    end_ts = time.time()
                    entry = ACTIVE_STREAMS.get(stream_id) or registry_entry
                    total_bytes = entry.get("total_bytes", 0)
                    start_ts = entry.get("start_ts", end_ts)
                    duration = end_ts - start_ts if end_ts > start_ts else 0.0
                    avg_mbps = (total_bytes / (1024 * 1024)) / (duration if duration > 0 else 1e-6)

                    part_count_value = int(entry.get("part_count", 0) or 0)
                    timeout_count = int(entry.get("chunk_timeouts", 0) or 0)
                    zero_pad_count = int(entry.get("zero_pad_chunks", 0) or 0)
                    buffering_events = timeout_count + zero_pad_count
                    buffering_rate = (buffering_events / part_count_value) if part_count_value > 0 else 0.0

                    entry.update({
                        "end_ts": end_ts,
                        "duration": duration,
                        "avg_mbps": avg_mbps,
                        "status": "finished" if entry.get("status") == "active" else entry.get("status", "finished"),
                        "parallelism": parallelism,
                        "buffering_events": buffering_events,
                        "buffering_rate": round(buffering_rate, 4),
                    })

                    # --- SLO warning logs (visibility only) ---
                    try:
                        ttfb = entry.get("ttfb_sec")
                        if isinstance(ttfb, (int, float)) and ttfb > float(getattr(Telegram, "STREAM_SLO_TTFB_WARN_SEC", 3.0) or 3.0):
                            LOGGER.warning(
                                "SLO warning: slow TTFB=%.3fs stream=%s client=%s dc=%s title=%s",
                                float(ttfb),
                                entry.get("stream_id"),
                                entry.get("client_index"),
                                entry.get("dc_id"),
                                (entry.get("meta", {}) or {}).get("title"),
                            )

                        timeout_warn = int(getattr(Telegram, "STREAM_SLO_TIMEOUT_WARN_COUNT", 2) or 2)
                        timeouts = int(entry.get("chunk_timeouts", 0) or 0)
                        if timeout_warn > 0 and timeouts >= timeout_warn:
                            LOGGER.warning(
                                "SLO warning: chunk_timeouts=%s stream=%s client=%s dc=%s",
                                timeouts,
                                entry.get("stream_id"),
                                entry.get("client_index"),
                                entry.get("dc_id"),
                            )

                        buffering_warn_rate = float(getattr(Telegram, "STREAM_SLO_BUFFERING_WARN_RATE", 0.05) or 0.05)
                        if buffering_warn_rate > 0 and buffering_rate >= buffering_warn_rate:
                            LOGGER.warning(
                                "SLO warning: buffering_rate=%.3f stream=%s client=%s dc=%s",
                                buffering_rate,
                                entry.get("stream_id"),
                                entry.get("client_index"),
                                entry.get("dc_id"),
                            )
                    except Exception:
                        pass

                    # --- Update rolling average speed for this client ---
                    prev = client_avg_mbps.get(client_index, 0.0)
                    if prev == 0.0:
                        client_avg_mbps[client_index] = avg_mbps
                    else:
                        # Exponential moving average: 30% new, 70% history
                        client_avg_mbps[client_index] = 0.7 * prev + 0.3 * avg_mbps
                    update_client_dc_metrics(client_index, int(getattr(file_id, "dc_id", 0) or 0), avg_mbps, entry.get("ttfb_sec"))
                    
                    # --- Log Analytics to DB ---
                    entry["chunk_size"] = chunk_size
                    asyncio.create_task(db.log_stream_stats(entry))

                    async def delayed_pop():
                        await asyncio.sleep(3)
                        try:
                            if stream_id in ACTIVE_STREAMS:
                                RECENT_STREAMS.appendleft(ACTIVE_STREAMS.pop(stream_id))
                        except Exception:
                            pass
                    
                    asyncio.create_task(delayed_pop())
                finally:
                    try:
                        work_loads[client_index] -= 1
                    except Exception:
                        pass

                stop_event.set()

        return consumer_generator()

    async def probe_file(
        self,
        chat_id: int,
        message_id: int,
        offset: int = 0,
        limit: int = 256 * 1024,
        timeout: float = 4.0,
    ) -> dict:
        """Fetch a tiny range to measure this helper against the file DC."""
        result = {
            "client_index": self.client_index,
            "ok": False,
            "file_id": None,
            "target_dc": None,
            "bytes": 0,
            "ttfb_sec": None,
            "mbps": 0.0,
            "error": None,
        }

        try:
            file_id = await self.get_file_properties(chat_id=chat_id, message_id=message_id)
            media_session = await self._get_media_session(file_id)
            location = await self._get_location(file_id)

            target_dc = int(getattr(file_id, "dc_id", 0) or 0)
            result["file_id"] = file_id
            result["target_dc"] = target_dc

            offset = max(0, int(offset or 0))
            limit = max(4096, min(int(limit or 0), 1024 * 1024))
            timeout = max(1.0, float(timeout or 4.0))

            started = time.perf_counter()
            chunk = await asyncio.wait_for(
                self._fetch_file_bytes(
                    media_session=media_session,
                    location=location,
                    offset=offset,
                    limit=limit,
                ),
                timeout=timeout,
            )
            elapsed = max(time.perf_counter() - started, 1e-6)
            size = len(chunk or b"")
            if size <= 0:
                result["error"] = "empty probe"
                return result

            mbps = (size / (1024 * 1024)) / elapsed
            result.update(
                {
                    "ok": True,
                    "bytes": size,
                    "ttfb_sec": elapsed,
                    "mbps": mbps,
                }
            )
            update_client_dc_metrics(self.client_index, target_dc, mbps, elapsed)
            return result
        except Exception as e:
            result["error"] = str(getattr(e, "args", e))
            return result

    async def _get_media_session(self, file_id: FileId) -> Session:
        dc = file_id.dc_id
        media_session = self.client.media_sessions.get(dc)

        if media_session:
            return media_session

        async with self._session_lock:
            media_session = self.client.media_sessions.get(dc)
            if media_session:
                return media_session

            test_mode = await self.client.storage.test_mode()
            current_dc = await self.client.storage.dc_id()

            if dc != current_dc:
                auth_key = await Auth(self.client, dc, test_mode).create()
            else:
                auth_key = await self.client.storage.auth_key()

            session = Session(self.client, dc, auth_key, test_mode, is_media=True)
            session.no_updates = True
            session.timeout = 30 
            session.sleep_threshold = 60 

            await session.start()

            if dc != current_dc:
                for _ in range(6):
                    try:
                        exported = await self.client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc))
                        await session.send(raw.functions.auth.ImportAuthorization(id=exported.id, bytes=exported.bytes))
                        break
                    except AuthBytesInvalid:
                        LOGGER.debug("AuthBytesInvalid during media session import; retrying...")
                        await asyncio.sleep(0.5)
                    except OSError:
                        LOGGER.debug("OSError during media session import; retrying...")
                        await asyncio.sleep(1)

            self.client.media_sessions[dc] = session
            LOGGER.debug("Created media session for DC %s", dc)
            return session

    @staticmethod
    async def _get_location(file_id: FileId) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
        ftype = file_id.file_type

        if ftype == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(user_id=file_id.chat_id, access_hash=file_id.chat_access_hash)
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(channel_id=utils.get_channel_id(file_id.chat_id),
                                                    access_hash=file_id.chat_access_hash)

            return raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )

        if ftype == FileType.PHOTO:
            return raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

        return raw.types.InputDocumentFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size=file_id.thumbnail_size,
        )

    async def _clean_cache(self) -> None:
        while True:
            await asyncio.sleep(self.CLEAN_INTERVAL)
            self._file_id_cache.clear()
            LOGGER.debug("ByteStreamer: cleared file_id cache")


# ---------------------------------------------------------------------------
# Speed Test helper – runs independently, on-demand per file
# ---------------------------------------------------------------------------

TEST_CHUNK_SIZE = 100 * 1024 * 1024  # 100 MB per test download


async def _speed_test_single_client(
    client: Client,
    client_index: int,
    chat_id: int,
    message_id: int,
    progress_callback=None,
) -> dict:
    """
    Benchmark one client: fetch a FRESH FileId (file reference is per-session),
    then measure ping (time-to-first-byte) and download throughput for
    TEST_CHUNK_SIZE bytes of the target file.
    """
    dc_id = client_dc_map.get(client_index, "?")
    result = {
        "client_index": client_index,
        "dc_id": dc_id,
        "ping_ms": None,
        "speed_mbps": None,
        "time_taken_sec": None,
        "bytes_downloaded": 0,
        "error": None,
    }
    try:
        # Each client MUST fetch its own FileId — file references are
        # per-session and will raise FILE_REFERENCE_EXPIRED if shared.
        streamer = ByteStreamer(client)
        file_id = await streamer.get_file_properties(chat_id, message_id)

        media_session = await streamer._get_media_session(file_id)
        location = await ByteStreamer._get_location(file_id)

        # --- Ping: time to first byte ---
        ping_start = time.perf_counter()
        tiny = await streamer._fetch_file_bytes(media_session, location, offset=0, limit=4096)
        ping_end = time.perf_counter()
        ping_ms = (ping_end - ping_start) * 1000
        result["ping_ms"] = round(ping_ms, 2)

        if not tiny:
            result["error"] = "No data on ping probe"
            return result

        # --- Download: TEST_CHUNK_SIZE bytes with concurrency ---
        dl_start = time.perf_counter()
        last_progress_time = dl_start
        total_bytes = 0
        
        chunk_size = 512 * 1024  # 512 KB per request
        max_concurrent_chunks = 8 # Telegram caps around 8-10 connections/requests
        
        queue = asyncio.Queue()
        # Seed the queue with offsets from 0 to TEST_CHUNK_SIZE
        target_offsets = list(range(0, TEST_CHUNK_SIZE, chunk_size))
        for off in target_offsets:
            queue.put_nowait(off)
            
        eof_reached = False
        
        async def fetch_chunk_worker():
            nonlocal total_bytes, last_progress_time, eof_reached
            while not queue.empty() and not eof_reached:
                offset = queue.get_nowait()
                fetch_size = min(chunk_size, TEST_CHUNK_SIZE - offset)
                
                try:
                    chunk = await asyncio.wait_for(
                        streamer._fetch_file_bytes(
                            media_session,
                            location,
                            offset=offset,
                            limit=fetch_size,
                        ),
                        timeout=15.0,
                    )
                    if not chunk:
                        eof_reached = True
                        queue.task_done()
                        continue
                        
                    bytes_got = len(chunk)
                    total_bytes += bytes_got
                    
                    if bytes_got < fetch_size:
                        eof_reached = True  # Natural EOF
                        
                    # Fire progress callback roughly every 1 second
                    now = time.perf_counter()
                    if progress_callback and (now - last_progress_time) >= 1.0:
                        elapsed_so_far = now - dl_start
                        if elapsed_so_far > 0:
                            current_speed = (total_bytes / (1024 * 1024)) / elapsed_so_far
                            prog_res = dict(result)
                            prog_res["bytes_downloaded"] = total_bytes
                            prog_res["time_taken_sec"] = round(elapsed_so_far, 3)
                            prog_res["speed_mbps"] = round(current_speed, 3)
                            
                            # Fire and forget callback (create_task) since we're in a worker
                            if asyncio.iscoroutinefunction(progress_callback):
                                asyncio.create_task(progress_callback(prog_res))
                            else:
                                progress_callback(prog_res)
                        last_progress_time = now

                except asyncio.TimeoutError:
                    # Expected during a speed probe — Telegram throttled or DC is slow.
                    # Log at DEBUG so it doesn't pollute the production error log.
                    LOGGER.debug(
                        "Speed-test chunk timeout client=%s offset=%s (skipping)",
                        client_index, offset,
                    )
                except Exception as e:
                    # Other transient errors (FloodWait, network blip, etc.)
                    LOGGER.debug(
                        "Speed-test fetch error client=%s offset=%s: %s",
                        client_index, offset, e,
                    )
                    
                finally:
                    queue.task_done()

        # Spawn workers
        workers = [
            asyncio.create_task(fetch_chunk_worker())
            for _ in range(max_concurrent_chunks)
        ]
        
        await queue.join()
        for w in workers:
            w.cancel()

        dl_end = time.perf_counter()
        elapsed = dl_end - dl_start
        if elapsed <= 0:
            elapsed = 1e-6

        speed_mbps = (total_bytes / (1024 * 1024)) / elapsed
        result["bytes_downloaded"] = total_bytes
        result["time_taken_sec"] = round(elapsed, 3)
        result["speed_mbps"] = round(speed_mbps, 3)

    except Exception as exc:
        result["error"] = str(exc)
        LOGGER.warning("Speed test failed for client %s (DC %s): %s", client_index, dc_id, exc)

    return result


async def run_speed_test(chat_id: int, message_id: int) -> List[dict]:
    """
    Run a parallel speed test against all active bot clients for the file
    identified by (chat_id, message_id).

    Each client fetches its own fresh FileId to avoid FILE_REFERENCE_EXPIRED.
    Returns a list of per-client result dicts sorted by speed descending.
    """
    if not multi_clients:
        return [{"error": "No bot clients connected"}]

    tasks = [
        _speed_test_single_client(client, idx, chat_id, message_id)
        for idx, client in multi_clients.items()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=False)
    results.sort(
        key=lambda r: r.get("speed_mbps") or -1,
        reverse=True,
    )
    return list(results)
