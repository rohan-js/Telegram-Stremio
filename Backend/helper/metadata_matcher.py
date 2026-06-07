import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover - optional runtime acceleration
    fuzz = None


NOISE_WORDS = {
    "amzn", "amazon", "nf", "netflix", "zee5", "hotstar", "jio", "sony", "liv",
    "web", "webdl", "web-dl", "webrip", "dl", "hdrip", "brrip", "bluray", "blu-ray",
    "hdtv", "dvdrip", "remux", "proper", "repack", "extended", "uncut",
    "h264", "h265", "x264", "x265", "hevc", "av1", "10bit", "8bit",
    "ddp", "dd", "aac", "atmos", "dts", "truehd", "5.1", "7.1",
    "1080p", "720p", "2160p", "480p", "4k", "uhd", "hdr", "dv",
    "tamil", "telugu", "malayalam", "hindi", "english", "kannada", "bengali",
    "mkv", "mp4", "avi", "webm", "mov", "flv", "wmv", "m4v",
}

GENERIC_TITLES = {
    "patriot", "war", "master", "hero", "leo", "animal", "jawan", "king",
    "queen", "love", "life", "ghost", "beast", "vikram", "don", "boss",
}


@dataclass
class MatchIntent:
    raw_title: str
    clean_title: str
    year: int | None
    media_type: str
    season: int | None = None
    episode: int | None = None
    season_pack: bool = False
    quality: str | None = None


@dataclass
class MatchCandidate:
    source: str
    title: str
    year: int | None
    media_type: str
    imdb_id: str | None = None
    tmdb_id: int | str | None = None
    popularity: float = 0.0
    raw: Any = None


@dataclass
class MatchDecision:
    accepted: bool
    candidate: MatchCandidate | None
    confidence: float
    reason: str
    candidates: list[dict]


def normalize_title(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"\b(?:19|20)\d{2}\b", " ", value)
    value = re.sub(r"\bs\d{1,2}e\d{1,2}\b", " ", value)
    value = re.sub(r"\bs\d{1,2}\b", " ", value)
    value = re.sub(r"[\._\-+\[\]\(\)\{\}:;,/\\|]+", " ", value)
    words = []
    for word in re.findall(r"[a-z0-9]+(?:'[a-z0-9]+)?", value):
        if word in NOISE_WORDS:
            continue
        if len(word) == 1:
            continue
        if word.isdigit() and len(word) <= 2:
            continue
        if re.fullmatch(r"\d{3,4}p?", word):
            continue
        words.append(word)
    return " ".join(words).strip()


def title_similarity(left: str | None, right: str | None) -> float:
    left_norm = normalize_title(left)
    right_norm = normalize_title(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 100.0
    if fuzz:
        return float(max(
            fuzz.ratio(left_norm, right_norm),
            fuzz.token_sort_ratio(left_norm, right_norm),
            fuzz.token_set_ratio(left_norm, right_norm),
        ))
    return SequenceMatcher(None, left_norm, right_norm).ratio() * 100.0


def is_generic_title(title: str | None) -> bool:
    normalized = normalize_title(title)
    return normalized in GENERIC_TITLES or len(normalized.split()) <= 1


def _year_score(intent_year: int | None, candidate_year: int | None) -> tuple[float, str | None]:
    if not intent_year:
        return 0.0, None
    if not candidate_year:
        return -12.0, "metadata_year_missing"
    delta = abs(int(intent_year) - int(candidate_year))
    if delta == 0:
        return 18.0, None
    if delta == 1:
        return 8.0, None
    return -80.0, "metadata_year_mismatch"


def _candidate_public(candidate: MatchCandidate, score: float, title_score: float, reason: str | None) -> dict:
    return {
        "source": candidate.source,
        "title": candidate.title,
        "year": candidate.year,
        "media_type": candidate.media_type,
        "imdb_id": candidate.imdb_id,
        "tmdb_id": candidate.tmdb_id,
        "score": round(score, 2),
        "title_score": round(title_score, 2),
        "reason": reason,
    }


def _same_identity(left: MatchCandidate, right: MatchCandidate) -> bool:
    if left.imdb_id and right.imdb_id and left.imdb_id == right.imdb_id:
        return True
    if left.tmdb_id and right.tmdb_id and str(left.tmdb_id) == str(right.tmdb_id):
        return True
    return (
        left.media_type == right.media_type
        and normalize_title(left.title) == normalize_title(right.title)
        and left.year == right.year
    )


def choose_best_candidate(intent: MatchIntent, candidates: list[MatchCandidate]) -> MatchDecision:
    scored: list[tuple[float, float, str | None, MatchCandidate]] = []
    rejected_reason = "metadata_no_candidates"

    for candidate in candidates:
        reason = None
        if candidate.media_type != intent.media_type:
            reason = "metadata_media_type_mismatch"
        title_score = title_similarity(intent.clean_title or intent.raw_title, candidate.title)
        year_bonus, year_reason = _year_score(intent.year, candidate.year)
        if year_reason:
            reason = year_reason

        score = title_score + year_bonus + min(float(candidate.popularity or 0.0), 20.0) / 20.0
        if reason:
            score -= 100.0
        scored.append((score, title_score, reason, candidate))

    scored.sort(key=lambda item: item[0], reverse=True)
    public = [_candidate_public(candidate, score, title_score, reason) for score, title_score, reason, candidate in scored[:8]]

    if not scored:
        return MatchDecision(False, None, 0.0, rejected_reason, public)

    top_score, top_title_score, top_reason, top = scored[0]
    second_score = -999.0
    for score, _, _, candidate in scored[1:]:
        if not _same_identity(top, candidate):
            second_score = score
            break
    margin = top_score - second_score
    generic = is_generic_title(intent.clean_title)

    if top_reason:
        return MatchDecision(False, top, top_score, top_reason, public)
    if top.media_type != intent.media_type:
        return MatchDecision(False, top, top_score, "metadata_media_type_mismatch", public)
    if intent.year and top.year and abs(int(intent.year) - int(top.year)) > 1:
        return MatchDecision(False, top, top_score, "metadata_year_mismatch", public)
    if intent.year and not top.year:
        return MatchDecision(False, top, top_score, "metadata_year_missing", public)
    if generic and not intent.year:
        return MatchDecision(False, top, top_score, "metadata_generic_title_needs_year", public)

    min_title = 92.0 if generic else 86.0
    min_margin = 8.0 if intent.year else 12.0
    if generic:
        min_margin = 12.0

    if top_title_score < min_title:
        return MatchDecision(False, top, top_score, "metadata_title_mismatch", public)
    if second_score > -999.0 and margin < min_margin:
        return MatchDecision(False, top, top_score, "metadata_ambiguous_match", public)

    return MatchDecision(True, top, top_score, "accepted", public)


def decision_metadata(decision: MatchDecision, intent: MatchIntent) -> dict:
    return {
        "parsed_title": intent.clean_title,
        "parsed_year": intent.year,
        "parsed_media_type": intent.media_type,
        "match_confidence": round(decision.confidence, 2),
        "match_rejection_reason": None if decision.accepted else decision.reason,
        "match_candidates": decision.candidates,
    }
