from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

# ---------------------------
# Quality Detail Schema
# ---------------------------
class QualityPart(BaseModel):
    part_number: int
    chat_id: int
    msg_id: int
    size_bytes: int


class QualityDetail(BaseModel):
    quality: str
    id: str
    name: str
    size: str
    group_key: Optional[str] = None
    parts: Optional[List[QualityPart]] = None
    source_type: str = "telegram"
    info_hash: Optional[str] = None
    file_idx: Optional[int] = None
    sources: Optional[List[str]] = None
    filename: Optional[str] = None
    video_size: Optional[int] = None
    origin_chat_id: Optional[int] = None
    origin_msg_id: Optional[int] = None
    torrent_private: bool = False
    torrent_source_uri: Optional[str] = None
    torrent_file_chat_id: Optional[int] = None
    torrent_file_msg_id: Optional[int] = None
    hidden_from_stremio: bool = False
    recommended: bool = False
    quality_note: Optional[str] = None
    flagged_duplicate: bool = False
    auto_matched: bool = False
    match_confidence: Optional[float] = None
    match_reason: Optional[str] = None
    match_candidates: Optional[List[dict]] = None
    rerank_used: bool = False
    rerank_timeout: bool = False
    rerank_cached: bool = False
    rerank_provider: Optional[str] = None
    rerank_model: Optional[str] = None
    rerank_confidence: Optional[int] = None
    rerank_reason: Optional[str] = None
    rerank_selected_candidate_index: Optional[int] = None
    gemini_used: bool = False
    gemini_timeout: bool = False
    gemini_cached: bool = False
    gemini_model: Optional[str] = None
    gemini_confidence: Optional[int] = None
    gemini_reason: Optional[str] = None
    gemini_selected_candidate_index: Optional[int] = None
    deterministic_match_reason: Optional[str] = None
    deterministic_match_confidence: Optional[float] = None


# ---------------------------
# Episode Schema
# ---------------------------
class Episode(BaseModel):
    episode_number: int
    title: str
    episode_backdrop: Optional[str] = None
    overview: Optional[str] = None
    released: Optional[str] = None
    telegram: Optional[List[QualityDetail]]


# ---------------------------
# Season Schema
# ---------------------------
class Season(BaseModel):
    season_number: int
    episodes: List[Episode] = Field(default_factory=list)


# ---------------------------
# TV Show Schema
# ---------------------------
class TVShowSchema(BaseModel):
    tmdb_id: Optional[int] = None
    imdb_id: Optional[str] = None
    db_index: int
    title: str
    genres: Optional[List[str]] = None
    description: Optional[str] = None
    rating: Optional[float] = None
    release_year: Optional[int] = None
    poster: Optional[str] = None
    backdrop: Optional[str] = None
    logo: Optional[str] = None
    cast: Optional[List[str]] = None
    runtime: Optional[str] = None
    media_type: str
    updated_on: datetime = Field(default_factory=datetime.utcnow)
    seasons: List[Season] = Field(default_factory=list)
    is_anime: Optional[bool] = False
    original_language: Optional[str] = None
    origin_country: Optional[List[str]] = Field(default_factory=list)
    production_countries: Optional[List[str]] = Field(default_factory=list)
    watch_providers: Optional[List[str]] = Field(default_factory=list)
    auto_tags: Optional[List[str]] = Field(default_factory=list)
    auto_catalog: Optional[dict] = None
    visibility: Optional[str] = "public"
    allowed_tokens: Optional[List[str]] = Field(default_factory=list)
    exclusive_catalog_id: Optional[str] = None
    exclusive_searchable: Optional[bool] = False


# ---------------------------
# Movie Schema
# ---------------------------
class MovieSchema(BaseModel):
    tmdb_id: Optional[int] = None
    imdb_id: Optional[str] = None
    db_index: int
    title: str
    genres: Optional[List[str]] = None
    description: Optional[str] = None
    rating: Optional[float] = None
    release_year: Optional[int] = None
    poster: Optional[str] = None
    backdrop: Optional[str] = None
    logo: Optional[str] = None
    cast: Optional[List[str]] = None
    runtime: Optional[str] = None
    media_type: str
    updated_on: datetime = Field(default_factory=datetime.utcnow)
    telegram: Optional[List[QualityDetail]]
    is_anime: Optional[bool] = False
    original_language: Optional[str] = None
    origin_country: Optional[List[str]] = Field(default_factory=list)
    production_countries: Optional[List[str]] = Field(default_factory=list)
    watch_providers: Optional[List[str]] = Field(default_factory=list)
    auto_tags: Optional[List[str]] = Field(default_factory=list)
    auto_catalog: Optional[dict] = None
    visibility: Optional[str] = "public"
    allowed_tokens: Optional[List[str]] = Field(default_factory=list)
    exclusive_catalog_id: Optional[str] = None
    exclusive_searchable: Optional[bool] = False
