import asyncio
import hashlib
import json
import re
import time
from collections import OrderedDict
from typing import Any

import httpx

from Backend.config import Telegram
from Backend.helper.metadata_matcher import MatchCandidate, MatchDecision, MatchIntent
from Backend.logger import LOGGER


GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
GROQ_API_BASE = "https://api.groq.com/openai/v1"
_CACHE: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_CLIENT: httpx.AsyncClient | None = None


def _has_gemini() -> bool:
    return bool(Telegram.GEMINI_API_KEY and Telegram.GEMINI_MATCHER_MODEL)


def _has_groq() -> bool:
    return bool(Telegram.GROQ_API_KEY and Telegram.GROQ_MATCHER_MODEL)


def _provider_order() -> list[str]:
    provider = (Telegram.METADATA_RERANKER_PROVIDER or "gemini").strip().lower()
    if provider not in {"auto", "groq", "gemini"}:
        provider = "auto"

    order: list[str] = []
    if provider == "groq":
        order = ["groq", "gemini"]
    elif provider == "gemini":
        order = ["gemini", "groq"]
    else:
        order = ["groq", "gemini"]

    available = []
    for item in order:
        if item == "groq" and _has_groq():
            available.append(item)
        elif item == "gemini" and _has_gemini():
            available.append(item)
    return available


def _is_enabled() -> bool:
    return bool(Telegram.GEMINI_MATCHER_ENABLED and _provider_order())


def _models_for_provider(provider: str) -> list[str]:
    if provider == "groq":
        models = [Telegram.GROQ_MATCHER_MODEL, Telegram.GROQ_MATCHER_FALLBACK_MODEL]
    else:
        models = [Telegram.GEMINI_MATCHER_MODEL, Telegram.GEMINI_MATCHER_FALLBACK_MODEL]

    result: list[str] = []
    for model in models:
        model = (model or "").strip()
        if model and model not in result:
            result.append(model)
    return result


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
            limits=httpx.Limits(max_connections=6, max_keepalive_connections=3),
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


def _gemini_request_body(prompt: str) -> dict:
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


def _groq_request_body(model: str, prompt: str) -> dict:
    return {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "Return only compact JSON with selected_candidate_index, confidence, and reason. Select only from the provided candidates.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "max_tokens": 80,
        "response_format": {"type": "json_object"},
        "stream": False,
    }


def _loads_json_text(text: str) -> dict:
    text = (text or "").strip()
    if not text:
        raise ValueError("LLM response did not include text")
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text).strip()
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start:end + 1]
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("LLM response JSON was not an object")
    return data


def _extract_gemini_json(response_data: dict) -> dict:
    parts = (
        response_data.get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [])
    )
    text = "".join(str(part.get("text") or "") for part in parts).strip()
    return _loads_json_text(text)


def _extract_groq_json(response_data: dict) -> dict:
    message = (
        response_data.get("choices", [{}])[0]
        .get("message", {})
    )
    return _loads_json_text(str(message.get("content") or ""))


def _timeout(timeout: float) -> httpx.Timeout:
    return httpx.Timeout(
        timeout,
        connect=min(0.35, timeout),
        read=timeout,
        write=min(0.35, timeout),
        pool=min(0.1, timeout),
    )


async def _request_gemini_model(model: str, prompt: str, timeout: float) -> dict:
    client = await _get_client()
    url = f"{GEMINI_API_BASE}/{model}:generateContent"
    response = await client.post(
        url,
        params={"key": Telegram.GEMINI_API_KEY},
        json=_gemini_request_body(prompt),
        timeout=_timeout(timeout),
    )
    response.raise_for_status()
    return _extract_gemini_json(response.json())


async def _request_groq_model(model: str, prompt: str, timeout: float) -> dict:
    client = await _get_client()
    response = await client.post(
        f"{GROQ_API_BASE}/chat/completions",
        headers={"Authorization": f"Bearer {Telegram.GROQ_API_KEY}"},
        json=_groq_request_body(model, prompt),
        timeout=_timeout(timeout),
    )
    response.raise_for_status()
    return _extract_groq_json(response.json())


async def _request_provider_model(provider: str, model: str, prompt: str, timeout: float) -> dict:
    if provider == "groq":
        return await _request_groq_model(model, prompt, timeout)
    return await _request_gemini_model(model, prompt, timeout)


async def _request_with_optional_fallback(prompt: str, timeout: float) -> tuple[dict, str, str]:
    last_error: Exception | None = None
    for provider in _provider_order():
        for model in _models_for_provider(provider):
            try:
                return await _request_provider_model(provider, model, prompt, timeout), provider, model
            except httpx.HTTPStatusError as exc:
                last_error = exc
                LOGGER.debug(
                    "Metadata rerank provider model failed: provider=%s model=%s status=%s",
                    provider,
                    model,
                    exc.response.status_code,
                )
                continue
            except Exception as exc:
                last_error = exc
                raise
    raise last_error or RuntimeError("Metadata rerank failed")


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


def _primary_provider() -> str:
    order = _provider_order()
    return order[0] if order else ""


def _primary_model() -> str:
    provider = _primary_provider()
    models = _models_for_provider(provider) if provider else []
    return models[0] if models else ""


def _diagnostic_extra(
    *,
    used: bool,
    timeout: bool = False,
    cached: bool = False,
    provider: str = "",
    model: str = "",
    reason: str = "",
    confidence: Any = None,
    selected_index: int | None = None,
    deterministic_reason: str | None = None,
    deterministic_confidence: float | None = None,
) -> dict:
    extra = {
        "rerank_used": used,
        "rerank_timeout": timeout,
        "rerank_cached": cached,
        "rerank_provider": provider,
        "rerank_model": model,
        "rerank_confidence": confidence,
        "rerank_reason": str(reason or "")[:500],
        "rerank_selected_candidate_index": selected_index,
    }
    if deterministic_reason is not None:
        extra["deterministic_match_reason"] = deterministic_reason
    if deterministic_confidence is not None:
        extra["deterministic_match_confidence"] = deterministic_confidence

    # Backward-compatible diagnostics for older database/UI code.
    if provider == "gemini":
        extra.update({
            "gemini_used": used,
            "gemini_timeout": timeout,
            "gemini_cached": cached,
            "gemini_model": model,
            "gemini_confidence": confidence,
            "gemini_reason": str(reason or "")[:500],
            "gemini_selected_candidate_index": selected_index,
        })
    return extra


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
            provider = result.get("provider") or _primary_provider()
            model = result.get("model") or _primary_model()
            from_cache = True
        else:
            result, provider, model = await asyncio.wait_for(
                _request_with_optional_fallback(_prompt(intent, public_candidates), timeout),
                timeout=timeout,
            )
            result = dict(result)
            result["provider"] = provider
            result["model"] = model
            _set_cached(key, result)
            from_cache = False

        selected_index = result.get("selected_candidate_index")
        if not isinstance(selected_index, int) or selected_index < 0 or selected_index >= len(public_candidates):
            return _with_extra(decision, _diagnostic_extra(
                used=False,
                provider=provider,
                model=model,
                reason="invalid_selected_candidate_index",
            ) | {"rerank_invalid": True})

        selected_public = public_candidates[selected_index]
        if selected_public.get("media_type") != intent.media_type:
            return _with_extra(decision, _diagnostic_extra(
                used=False,
                provider=provider,
                model=model,
                reason="selected_media_type_mismatch",
            ) | {"rerank_invalid": True})

        selected_candidate = _find_candidate(candidates, selected_public)
        if not selected_candidate:
            return _with_extra(decision, _diagnostic_extra(
                used=False,
                provider=provider,
                model=model,
                reason="selected_candidate_not_found",
            ) | {"rerank_invalid": True})

        confidence = float(selected_public.get("score") or decision.confidence)
        return MatchDecision(
            True,
            selected_candidate,
            confidence,
            "accepted_llm_rerank",
            decision.candidates,
            _diagnostic_extra(
                used=True,
                timeout=False,
                cached=from_cache,
                provider=provider,
                model=model,
                confidence=result.get("confidence"),
                reason=result.get("reason"),
                selected_index=selected_index,
                deterministic_reason=decision.reason,
                deterministic_confidence=decision.confidence,
            ),
        )
    except (asyncio.TimeoutError, httpx.TimeoutException):
        return _with_extra(decision, _diagnostic_extra(
            used=False,
            timeout=True,
            provider=_primary_provider(),
            model=_primary_model(),
            reason="timeout",
        ))
    except Exception as exc:
        LOGGER.debug("Metadata rerank skipped after error: %s", exc)
        return _with_extra(decision, _diagnostic_extra(
            used=False,
            provider=_primary_provider(),
            model=_primary_model(),
            reason="error",
        ) | {"rerank_error": type(exc).__name__})
