import asyncio
import hashlib
import json
import time
from collections import OrderedDict
from typing import Any

import httpx

from Backend.config import Telegram
from Backend.helper.metadata_matcher import MatchCandidate, MatchDecision, MatchIntent
from Backend.logger import LOGGER


GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
_CACHE: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_CLIENT: httpx.AsyncClient | None = None


def _is_enabled() -> bool:
    return bool(
        Telegram.GEMINI_MATCHER_ENABLED
        and Telegram.GEMINI_API_KEY
        and Telegram.GEMINI_MATCHER_MODEL
    )


def _candidate_key(value: dict | MatchCandidate | None) -> str:
    if not value:
        return ""
    getter = value.get if isinstance(value, dict) else lambda key, default=None: getattr(value, key, default)
    imdb_id = getter("imdb_id")
    tmdb_id = getter("tmdb_id")
    if imdb_id:
        return f"imdb:{imdb_id}"
    if tmdb_id:
        return f"tmdb:{tmdb_id}"
    return "::".join(str(getter(key, "") or "") for key in ("media_type", "title", "year", "source"))


def _top_margin(candidates: list[dict]) -> float | None:
    if len(candidates) < 2:
        return None
    top_key = _candidate_key(candidates[0])
    top_score = float(candidates[0].get("score") or 0.0)
    for candidate in candidates[1:]:
        if _candidate_key(candidate) != top_key:
            return top_score - float(candidate.get("score") or 0.0)
    return None


def should_use_gemini(decision: MatchDecision) -> bool:
    if not _is_enabled():
        return False
    if not decision.accepted or not decision.candidate:
        return False
    if not str(decision.reason).startswith("accepted_low_confidence:"):
        return False

    candidates = (decision.candidates or [])[:Telegram.GEMINI_MATCHER_MAX_CANDIDATES]
    if len(candidates) < 2:
        return False
    margin = _top_margin(candidates)
    return margin is not None and margin <= Telegram.GEMINI_MATCHER_MIN_TOP_MARGIN


def _cache_key(intent: MatchIntent, candidates: list[dict]) -> str:
    payload = {
        "raw_title": intent.raw_title,
        "clean_title": intent.clean_title,
        "year": intent.year,
        "media_type": intent.media_type,
        "season": intent.season,
        "episode": intent.episode,
        "season_pack": intent.season_pack,
        "candidates": [
            {
                "key": _candidate_key(candidate),
                "title": candidate.get("title"),
                "year": candidate.get("year"),
                "media_type": candidate.get("media_type"),
                "score": candidate.get("score"),
            }
            for candidate in candidates
        ],
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _get_cached(key: str) -> dict | None:
    if not Telegram.GEMINI_MATCHER_CACHE_MAX or not Telegram.GEMINI_MATCHER_CACHE_TTL_SECONDS:
        return None
    item = _CACHE.get(key)
    if not item:
        return None
    created_at, value = item
    if time.monotonic() - created_at > Telegram.GEMINI_MATCHER_CACHE_TTL_SECONDS:
        _CACHE.pop(key, None)
        return None
    _CACHE.move_to_end(key)
    return dict(value)


def _set_cached(key: str, value: dict) -> None:
    if not Telegram.GEMINI_MATCHER_CACHE_MAX or not Telegram.GEMINI_MATCHER_CACHE_TTL_SECONDS:
        return
    _CACHE[key] = (time.monotonic(), dict(value))
    _CACHE.move_to_end(key)
    while len(_CACHE) > Telegram.GEMINI_MATCHER_CACHE_MAX:
        _CACHE.popitem(last=False)


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
            follow_redirects=False,
        )
    return _CLIENT


def _prompt(intent: MatchIntent, candidates: list[dict]) -> str:
    payload = {
        "task": "Choose the best real metadata candidate for this media file. Select only from candidates. Do not invent metadata.",
        "file": {
            "name": intent.raw_title,
            "parsed_title": intent.clean_title,
            "year": intent.year,
            "media_type": intent.media_type,
            "season": intent.season,
            "episode": intent.episode,
            "season_pack": intent.season_pack,
        },
        "candidates": [
            {
                "index": index,
                "source": candidate.get("source"),
                "title": candidate.get("title"),
                "year": candidate.get("year"),
                "media_type": candidate.get("media_type"),
                "imdb_id": candidate.get("imdb_id"),
                "tmdb_id": candidate.get("tmdb_id"),
                "score": candidate.get("score"),
                "title_score": candidate.get("title_score"),
                "reason": candidate.get("reason"),
            }
            for index, candidate in enumerate(candidates)
        ],
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _request_body(prompt: str) -> dict:
    return {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0,
            "maxOutputTokens": 80,
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": {
                    "selected_candidate_index": {"type": "INTEGER"},
                    "confidence": {"type": "INTEGER"},
                    "reason": {"type": "STRING"},
                },
                "required": ["selected_candidate_index", "confidence", "reason"],
            },
        },
    }


def _extract_json(response_data: dict) -> dict:
    parts = (
        response_data.get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [])
    )
    text = "".join(str(part.get("text") or "") for part in parts).strip()
    if not text:
        raise ValueError("Gemini response did not include text")
    return json.loads(text)


async def _request_model(model: str, prompt: str, timeout: float) -> dict:
    client = await _get_client()
    url = f"{GEMINI_API_BASE}/{model}:generateContent"
    response = await client.post(
        url,
        params={"key": Telegram.GEMINI_API_KEY},
        json=_request_body(prompt),
        timeout=httpx.Timeout(timeout, connect=min(0.35, timeout), read=timeout, write=min(0.35, timeout), pool=min(0.1, timeout)),
    )
    response.raise_for_status()
    return _extract_json(response.json())


async def _request_with_optional_fallback(prompt: str, timeout: float) -> tuple[dict, str]:
    models = [Telegram.GEMINI_MATCHER_MODEL]
    fallback = Telegram.GEMINI_MATCHER_FALLBACK_MODEL
    if fallback and fallback not in models:
        models.append(fallback)

    last_error: Exception | None = None
    for model in models:
        try:
            return await _request_model(model, prompt, timeout), model
        except httpx.HTTPStatusError as exc:
            last_error = exc
            if exc.response.status_code not in {400, 404}:
                break
        except Exception as exc:
            last_error = exc
            break
    raise last_error or RuntimeError("Gemini rerank failed")


def _find_candidate(candidates: list[MatchCandidate], selected: dict) -> MatchCandidate | None:
    selected_key = _candidate_key(selected)
    for candidate in candidates:
        if _candidate_key(candidate) == selected_key:
            return candidate
    return None


def _with_extra(decision: MatchDecision, extra: dict) -> MatchDecision:
    current = dict(decision.extra or {})
    current.update(extra)
    return MatchDecision(
        decision.accepted,
        decision.candidate,
        decision.confidence,
        decision.reason,
        decision.candidates,
        current,
    )


async def maybe_rerank_with_gemini(
    intent: MatchIntent,
    decision: MatchDecision,
    candidates: list[MatchCandidate],
) -> MatchDecision:
    if not should_use_gemini(decision):
        return decision

    public_candidates = (decision.candidates or [])[:Telegram.GEMINI_MATCHER_MAX_CANDIDATES]
    key = _cache_key(intent, public_candidates)
    cached = _get_cached(key)
    timeout = Telegram.GEMINI_MATCHER_TIMEOUT_SECONDS

    try:
        if cached is not None:
            result = cached
            model = result.get("model") or Telegram.GEMINI_MATCHER_MODEL
            from_cache = True
        else:
            result, model = await asyncio.wait_for(
                _request_with_optional_fallback(_prompt(intent, public_candidates), timeout),
                timeout=timeout,
            )
            result = dict(result)
            result["model"] = model
            _set_cached(key, result)
            from_cache = False

        selected_index = result.get("selected_candidate_index")
        if not isinstance(selected_index, int) or selected_index < 0 or selected_index >= len(public_candidates):
            return _with_extra(decision, {
                "gemini_used": False,
                "gemini_invalid": True,
                "gemini_reason": "invalid_selected_candidate_index",
            })

        selected_public = public_candidates[selected_index]
        if selected_public.get("media_type") != intent.media_type:
            return _with_extra(decision, {
                "gemini_used": False,
                "gemini_invalid": True,
                "gemini_reason": "selected_media_type_mismatch",
            })

        selected_candidate = _find_candidate(candidates, selected_public)
        if not selected_candidate:
            return _with_extra(decision, {
                "gemini_used": False,
                "gemini_invalid": True,
                "gemini_reason": "selected_candidate_not_found",
            })

        confidence = float(selected_public.get("score") or decision.confidence)
        return MatchDecision(
            True,
            selected_candidate,
            confidence,
            "accepted_gemini_rerank",
            decision.candidates,
            {
                "gemini_used": True,
                "gemini_timeout": False,
                "gemini_cached": from_cache,
                "gemini_model": model,
                "gemini_confidence": result.get("confidence"),
                "gemini_reason": str(result.get("reason") or "")[:500],
                "gemini_selected_candidate_index": selected_index,
                "deterministic_match_reason": decision.reason,
                "deterministic_match_confidence": decision.confidence,
            },
        )
    except (asyncio.TimeoutError, httpx.TimeoutException):
        return _with_extra(decision, {
            "gemini_used": False,
            "gemini_timeout": True,
            "gemini_model": Telegram.GEMINI_MATCHER_MODEL,
            "gemini_reason": "timeout",
        })
    except Exception as exc:
        LOGGER.debug("Gemini metadata rerank skipped after error: %s", exc)
        return _with_extra(decision, {
            "gemini_used": False,
            "gemini_error": type(exc).__name__,
            "gemini_reason": "error",
        })
