"""Nuvio collection JSON endpoints (upstream fb33de8..9f0d68a lineage).

Builds the four Nuvio collection documents (Discover / Streaming Platforms /
Franchises & Universes / Regional & Spotlight) from the fork's catalog
universe: builtin latest/top shelves, custom catalogs and TMDB-driven auto
catalogs (matched by auto_key, e.g. auto_netflix). Folders whose catalogs
don't exist here are dropped silently (strict=True), so the shape degrades
gracefully to what this library actually has.

Fork adaptations vs upstream:
- visibility: fork's _token_can_view + _media_visible_to_token per item
- sanitize keeps tmdb/trakt remote folders alive (upstream's
  addon-only guard killed its own Franchises collection)
- trending: the auto_trending catalog instead of live trending_* catalogs
- art: self-hosted under /static/nuvio (no upstream CDN dependency)
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends

from Backend.config import Telegram
from Backend import db
from Backend.fastapi.security.tokens import verify_token
from Backend.fastapi.routes.stremio_routes import _media_visible_to_token, _token_can_view

router = APIRouter(prefix="/stremio", tags=["Nuvio"])

BASE_URL = Telegram.BASE_URL


#----- Catalog-level AND item-level visibility (upstream _effective_visibility
#----- equivalent, expressed through the fork's checks).
def _nuvio_item_visible(catalog: dict, item: dict, token_data: Optional[dict]) -> bool:
    if not _token_can_view(catalog.get("visibility") or "public", catalog.get("allowed_tokens") or [], token_data):
        return False
    return _media_visible_to_token(item, token_data, catalog_id=str(catalog.get("_id")))


async def build_nuvio_collection_data(token: str, collection_type: str = "full", token_data: Optional[dict] = None) -> list:
    addon_id = f"telegram.media.{token[:8]}"
    
    if token_data is None and token:
        try:
            token_data = await db.get_api_token(token)
        except Exception:
            token_data = None

    # Map catalog names to catalog IDs for this token, separated by media type
    catalog_movies_map = {}
    catalog_series_map = {}
    auto_key_movies_map = {}
    auto_key_series_map = {}

    try:
        custom_catalogs = await db.get_custom_catalogs(slim=True)
        for catalog in custom_catalogs:
            # Check visibility and items
            raw_items = catalog.get("items") or []
            visible_items = [
                i for i in raw_items
                if _nuvio_item_visible(catalog, i, token_data)
            ]

            has_movie = any(i.get("media_type") == "movie" for i in visible_items)
            has_series = any(i.get("media_type") == "tv" for i in visible_items)

            cid = f"custom_{catalog.get('_id')}"
            name = (catalog.get("name") or "").lower().strip()
            auto_key = (catalog.get("auto_key") or "").lower().strip()

            if has_movie:
                catalog_movies_map[name] = cid
                if auto_key:
                    auto_key_movies_map[auto_key] = cid
            if has_series:
                catalog_series_map[name] = cid
                if auto_key:
                    auto_key_series_map[auto_key] = cid
    except Exception:
        pass

    # Helper to get catalog ID for movies and series only if they actually have content
    def get_cat_id(name_query: str, auto_key: str = "", media_type: str = "movie", fallback: Optional[str] = None, strict: bool = True):
        cmap = catalog_movies_map if media_type == "movie" else catalog_series_map
        amap = auto_key_movies_map if media_type == "movie" else auto_key_series_map
        if auto_key and auto_key.lower() in amap:
            return amap[auto_key.lower()]
        for name, cid in cmap.items():
            if name_query.lower() in name:
                return cid
        return None if strict else fallback

    hotstar_movie = get_cat_id("hotstar", "auto_hotstar", "movie", strict=True)
    hotstar_series = get_cat_id("hotstar", "auto_hotstar", "series", strict=True)
    netflix_movie = get_cat_id("netflix", "auto_netflix", "movie", strict=True)
    netflix_series = get_cat_id("netflix", "auto_netflix", "series", strict=True)
    prime_movie = get_cat_id("prime", "auto_prime_video", "movie", strict=True)
    prime_series = get_cat_id("prime", "auto_prime_video", "series", strict=True)
    apple_movie = get_cat_id("apple", "auto_apple_tv", "movie", strict=True)
    apple_series = get_cat_id("apple", "auto_apple_tv", "series", strict=True)
    hbo_movie = get_cat_id("hbo", "auto_hbo", "movie", strict=True)
    hbo_series = get_cat_id("hbo", "auto_hbo", "series", strict=True)
    hulu_movie = get_cat_id("hulu", "auto_hulu", "movie", strict=True)
    hulu_series = get_cat_id("hulu", "auto_hulu", "series", strict=True)
    mx_movie = get_cat_id("mx", "auto_mx_player", "movie", strict=True)
    mx_series = get_cat_id("mx", "auto_mx_player", "series", strict=True)
    sony_movie = get_cat_id("sony", "auto_sonyliv", "movie", strict=True)
    sony_series = get_cat_id("sony", "auto_sonyliv", "series", strict=True)
    zee5_movie = get_cat_id("zee", "auto_zee5", "movie", strict=True)
    zee5_series = get_cat_id("zee", "auto_zee5", "series", strict=True)
    crunchyroll_movie = get_cat_id("crunchyroll", "auto_crunchyroll", "movie", strict=True)
    crunchyroll_series = get_cat_id("crunchyroll", "auto_crunchyroll", "series", strict=True)
    bollywood_movie = get_cat_id("bollywood", "auto_bollywood", "movie", strict=True)
    bollywood_series = get_cat_id("bollywood", "auto_bollywood", "series", strict=True)
    south_indian_movie = get_cat_id("south indian", "auto_south_indian", "movie", strict=True)
    south_indian_series = get_cat_id("south indian", "auto_south_indian", "series", strict=True)
    kdrama_movie = get_cat_id("korean", "auto_korean", "movie", strict=True)
    kdrama_series = get_cat_id("k-drama", "auto_kdrama", "series", strict=True) or get_cat_id("korean", "auto_korean", "series", strict=True)
    anime_movie = get_cat_id("anime", "auto_anime", "movie", strict=True)
    anime_series = get_cat_id("anime", "auto_anime", "series", strict=True)
    # Fork: TMDB weekly trending lives in the auto_trending catalog
    trending_movie = get_cat_id("trending now", "auto_trending", "movie", strict=True)
    trending_series = get_cat_id("trending now", "auto_trending", "series", strict=True)
    latest_releases_movie = get_cat_id("latest releases", "auto_latest_releases", "movie", strict=True)
    latest_releases_series = get_cat_id("latest releases", "auto_latest_releases", "series", strict=True)

    # =========================================================================
    # 01 ✦ DISCOVER COLLECTION (TMDB Global Trends + Local Addon Fallback)
    # =========================================================================
    K_IMG = "https://raw.githubusercontent.com/Kaoxt/The-Kollection/main/images"
    NUVIO_CDN = f"{BASE_URL}/static/nuvio"

    # =========================================================================
    # 01 ✦ DISCOVER COLLECTION (TMDB Global Trends + Local Addon Fallback)
    # =========================================================================
    discover_collection = {
        "id": "collection-discover-main",
        "title": "Discover",
        "folders": [
            {
                "id": "folder-discover-trending",
                "title": "Trending Now",
                "sources": [
                    {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": trending_movie},
                    {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": trending_series},
                    {"title": "Trending Movies", "sortBy": "popularity.desc", "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "DISCOVER"},
                    {"title": "Trending TV Series", "sortBy": "popularity.desc", "provider": "tmdb", "mediaType": "TV", "tmdbSourceType": "DISCOVER"}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{K_IMG}/Discover/Trending/logo.webp",
                "coverImageUrl": f"{K_IMG}/Discover/Trending/cover.webp",
                "heroBackdropUrl": f"{K_IMG}/Discover/Trending/backdrop.webp",
                "catalogSources": [
                    {"type": "movie", "addonId": addon_id, "catalogId": trending_movie},
                    {"type": "series", "addonId": addon_id, "catalogId": trending_series}
                ],
                "focusGifEnabled": False
            },
            {
                "id": "folder-discover-latest-releases",
                "title": "Latest Releases",
                "sources": [
                    {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": latest_releases_movie},
                    {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": latest_releases_series}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{K_IMG}/Discover/Premieres/logo.webp",
                "coverImageUrl": f"{K_IMG}/Discover/Premieres/cover.webp",
                "heroBackdropUrl": f"{K_IMG}/Discover/Premieres/backdrop.webp",
                "catalogSources": [
                    {"type": "movie", "addonId": addon_id, "catalogId": latest_releases_movie},
                    {"type": "series", "addonId": addon_id, "catalogId": latest_releases_series}
                ],
                "focusGifEnabled": False
            },
            {
                "id": "folder-discover-popular",
                "title": "Popular Hits",
                "sources": [
                    {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": "top_movies"},
                    {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": "top_series"},
                    {"title": "Popular Movies", "sortBy": "popularity.desc", "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "DISCOVER"},
                    {"title": "Popular Series", "sortBy": "popularity.desc", "provider": "tmdb", "mediaType": "TV", "tmdbSourceType": "DISCOVER"}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{K_IMG}/Discover/Popular/logo.webp",
                "coverImageUrl": f"{K_IMG}/Discover/Popular/cover.webp",
                "heroBackdropUrl": f"{K_IMG}/Discover/Popular/backdrop.webp",
                "catalogSources": [
                    {"type": "movie", "addonId": addon_id, "catalogId": "top_movies"},
                    {"type": "series", "addonId": addon_id, "catalogId": "top_series"}
                ],
                "focusGifEnabled": False
            },
            {
                "id": "folder-discover-latest",
                "title": "Recently Added",
                "sources": [
                    {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": "latest_movies"},
                    {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": "latest_series"},
                    {
                        "title": "Latest Released Movies",
                        "sortBy": "primary_release_date.desc",
                        "filters": {
                            "voteCountGte": 15,
                            "releaseDateGte": (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d"),
                            "releaseDateLte": datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        },
                        "provider": "tmdb",
                        "mediaType": "MOVIE",
                        "tmdbSourceType": "DISCOVER"
                    },
                    {
                        "title": "New TV Series Episodes",
                        "sortBy": "first_air_date.desc",
                        "filters": {
                            "voteCountGte": 10,
                            "releaseDateGte": (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d"),
                            "releaseDateLte": datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        },
                        "provider": "tmdb",
                        "mediaType": "TV",
                        "tmdbSourceType": "DISCOVER"
                    }
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{NUVIO_CDN}/RecentlyAdded_logo.webp",
                "coverImageUrl": f"{NUVIO_CDN}/RecentlyAdded_cover.webp",
                "heroBackdropUrl": f"{NUVIO_CDN}/RecentlyAdded_backdrop.webp",
                "catalogSources": [
                    {"type": "movie", "addonId": addon_id, "catalogId": "latest_movies"},
                    {"type": "series", "addonId": addon_id, "catalogId": "latest_series"}
                ],
                "focusGifEnabled": False
            }
        ],
        "pinToTop": True,
        "viewMode": "TABBED_GRID",
        "showAllTab": True,
        "focusGlowEnabled": True
    }

    # =========================================================================
    # 02 ✦ STREAMING PLATFORMS (OTT Hubs)
    # =========================================================================
    ott_collection = {
        "id": "collection-erfs5gwk-community",
        "title": "Streaming Platforms",
        "folders": [
          {
            "id": "folder-WFGY1XPD",
            "title": "Netflix",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": netflix_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": netflix_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "",
            "titleLogoUrl": f"{K_IMG}/Streaming%20Platforms/Netflix/logo.webp",
            "coverImageUrl": f"{NUVIO_CDN}/Netflix.webp",
            "heroBackdropUrl": f"{NUVIO_CDN}/Netflix.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": netflix_movie},
              {"type": "series", "addonId": addon_id, "catalogId": netflix_series}
            ],
            "focusGifEnabled": False
          },
          {
            "id": "folder-AT8KYHZO",
            "title": "Prime Video",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": prime_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": prime_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "",
            "titleLogoUrl": f"{K_IMG}/Streaming%20Platforms/Prime/logo.webp",
            "coverImageUrl": f"{NUVIO_CDN}/PrimeVideo.webp",
            "heroBackdropUrl": f"{NUVIO_CDN}/PrimeVideo.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": prime_movie},
              {"type": "series", "addonId": addon_id, "catalogId": prime_series}
            ],
            "focusGifEnabled": False
          },
          {
            "id": "folder-8ZBZOMW9",
            "title": "JioHotstar",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": hotstar_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": hotstar_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "",
            "titleLogoUrl": f"{K_IMG}/Streaming%20Platforms/Disney+/logo.webp",
            "coverImageUrl": f"{NUVIO_CDN}/JioHotstar.webp",
            "heroBackdropUrl": f"{NUVIO_CDN}/JioHotstar.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": hotstar_movie},
              {"type": "series", "addonId": addon_id, "catalogId": hotstar_series}
            ],
            "focusGifEnabled": False
          },
          {
            "id": "folder-IQTF27K8",
            "title": "Apple TV",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": apple_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": apple_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "",
            "titleLogoUrl": f"{K_IMG}/Streaming%20Platforms/Apple%20TV/logo.webp",
            "coverImageUrl": f"{NUVIO_CDN}/AppleTV.webp",
            "heroBackdropUrl": f"{NUVIO_CDN}/AppleTV.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": apple_movie},
              {"type": "series", "addonId": addon_id, "catalogId": apple_series}
            ],
            "focusGifEnabled": False
          },
          {
            "id": "folder-OA4T8NYO",
            "title": "HBO Max",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": hbo_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": hbo_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "",
            "titleLogoUrl": f"{K_IMG}/Streaming%20Platforms/HBO%20Max/logo.webp",
            "coverImageUrl": f"{NUVIO_CDN}/HBOMax.webp",
            "heroBackdropUrl": f"{NUVIO_CDN}/HBOMax.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": hbo_movie},
              {"type": "series", "addonId": addon_id, "catalogId": hbo_series}
            ],
            "focusGifEnabled": False
          },
          {
            "id": "folder-LN9SPMQN",
            "title": "Hulu",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": hulu_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": hulu_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "",
            "titleLogoUrl": f"{K_IMG}/Streaming%20Platforms/Hulu/logo.webp",
            "coverImageUrl": f"{NUVIO_CDN}/hulu.webp",
            "heroBackdropUrl": f"{NUVIO_CDN}/hulu.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": hulu_movie},
              {"type": "series", "addonId": addon_id, "catalogId": hulu_series}
            ],
            "focusGifEnabled": False
          },
          {
            "id": "folder-ZEE5-OTT",
            "title": "ZEE5",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": zee5_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": zee5_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "",
            "titleLogoUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/title/zee5.webp",
            "coverImageUrl": f"{NUVIO_CDN}/Zee5.webp",
            "heroBackdropUrl": f"{NUVIO_CDN}/Zee5.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": zee5_movie},
              {"type": "series", "addonId": addon_id, "catalogId": zee5_series}
            ],
            "focusGifEnabled": False
          },
          {
            "id": "folder-SONY-OTT",
            "title": "SonyLIV",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": sony_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": sony_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "",
            "titleLogoUrl": f"{K_IMG}/Studios/Sony%20Pictures/logo.webp",
            "coverImageUrl": f"{NUVIO_CDN}/SonyLiv.webp",
            "heroBackdropUrl": f"{NUVIO_CDN}/SonyLiv.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": sony_movie},
              {"type": "series", "addonId": addon_id, "catalogId": sony_series}
            ],
            "focusGifEnabled": False
          },
          {
            "id": "85a10640-79f2-435f-9d63-348e921e37f4",
            "title": "Crunchyroll",
            "sources": [
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": crunchyroll_series},
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": crunchyroll_movie}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "",
            "titleLogoUrl": f"{K_IMG}/Streaming%20Platforms/Crunchyroll/logo.webp",
            "coverImageUrl": f"{NUVIO_CDN}/Crunchyroll.webp",
            "heroBackdropUrl": f"{NUVIO_CDN}/Crunchyroll.webp",
            "catalogSources": [
              {"type": "series", "addonId": addon_id, "catalogId": crunchyroll_series},
              {"type": "movie", "addonId": addon_id, "catalogId": crunchyroll_movie}
            ],
            "focusGifEnabled": False
          }
        ],
        "pinToTop": True,
        "viewMode": "TABBED_GRID",
        "showAllTab": True,
        "focusGlowEnabled": True
    }

    # Sanitize OTT folders: keep only those with active sources and strip any null catalogIds
    cleaned_ott_folders = []
    for f in ott_collection.get("folders") or []:
        sources = [s for s in f.get("sources") or [] if s.get("catalogId")]
        catalog_sources = [cs for cs in f.get("catalogSources") or [] if cs.get("catalogId")]
        if sources:
            folder_copy = dict(f)
            folder_copy["sources"] = sources
            folder_copy["catalogSources"] = catalog_sources
            cleaned_ott_folders.append(folder_copy)
    ott_collection["folders"] = cleaned_ott_folders

    # =========================================================================
    # 03 ✦ FRANCHISES & UNIVERSES (Curated Sagas - Kaoxt The-Kollection)
    # =========================================================================
    franchises_collection = {
        "id": "collection-franchises-main",
        "title": "Franchises & Universes",
        "folders": [
            {
                "id": "folder-mcu-universe",
                "title": "Marvel Cinematic Universe",
                "sources": [
                    {"title": "Marvel Studios Movies", "sortBy": "primary_release_date.desc", "tmdbId": 420, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "COMPANY"},
                    {"title": "Marvel Studios Series", "sortBy": "primary_release_date.desc", "tmdbId": 420, "provider": "tmdb", "mediaType": "TV", "tmdbSourceType": "COMPANY"},
                    {"title": "Spider-Man Films", "sortBy": "rank", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 36436842},
                    {"title": "X-Men Universe", "sortBy": "original", "tmdbId": 27741, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "LIST"}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{K_IMG}/Studios/Marvel/logo.webp",
                "coverImageUrl": f"{K_IMG}/Studios/Marvel/cover.webp",
                "heroBackdropUrl": f"{K_IMG}/Studios/Marvel/backdrop.webp",
                "catalogSources": [],
                "focusGifEnabled": False
            },
            {
                "id": "folder-dcu-universe",
                "title": "DC Universe & Batman",
                "sources": [
                    {"title": "DC Extended Universe", "sortBy": "original", "tmdbId": 85568, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "LIST"},
                    {"title": "Batman Collection", "sortBy": "original", "tmdbId": 263, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "COLLECTION"},
                    {"title": "DC Animated Originals", "sortBy": "original", "tmdbId": 3255, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "LIST"}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{K_IMG}/Studios/DC/logo.webp",
                "coverImageUrl": f"{K_IMG}/Studios/DC/cover.webp",
                "heroBackdropUrl": f"{K_IMG}/Studios/DC/backdrop.webp",
                "catalogSources": [],
                "focusGifEnabled": False
            },
            {
                "id": "folder-starwars-universe",
                "title": "Star Wars Universe",
                "sources": [
                    {"title": "Star Wars Movies", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 22202556},
                    {"title": "Star Wars: Shows & Series", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "TV", "traktListId": 22757470}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{K_IMG}/Franchises/Star%20Wars/logo.webp",
                "coverImageUrl": f"{K_IMG}/Franchises/Star%20Wars/cover.webp",
                "heroBackdropUrl": f"{K_IMG}/Franchises/Star%20Wars/backdrop.webp",
                "catalogSources": [],
                "focusGifEnabled": False
            },
            {
                "id": "folder-james-bond-007",
                "title": "James Bond (007)",
                "sources": [
                    {"title": "The James Bond (007) Collection", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 1919354}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{K_IMG}/Franchises/007/logo.webp",
                "coverImageUrl": f"{K_IMG}/Franchises/007/cover.webp",
                "heroBackdropUrl": f"{K_IMG}/Franchises/007/backdrop.webp",
                "catalogSources": [],
                "focusGifEnabled": False
            },
            {
                "id": "folder-lotr-middleearth",
                "title": "Middle-Earth (LOTR & The Hobbit)",
                "sources": [
                    {"title": "The Lord of the Rings Collection", "sortBy": "original", "tmdbId": 119, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "COLLECTION"},
                    {"title": "The Hobbit Collection", "sortBy": "original", "tmdbId": 121938, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "COLLECTION"},
                    {"title": "Middle-Earth Saga", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 1642688}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{K_IMG}/Franchises/The%20Lord%20of%20the%20Rings/logo.webp",
                "coverImageUrl": f"{K_IMG}/Franchises/The%20Lord%20of%20the%20Rings/cover.webp",
                "heroBackdropUrl": f"{K_IMG}/Franchises/The%20Lord%20of%20the%20Rings/backdrop.webp",
                "catalogSources": [],
                "focusGifEnabled": False
            },
            {
                "id": "folder-harry-potter",
                "title": "Harry Potter & Wizarding World",
                "sources": [
                    {"title": "Harry Potter Collection", "sortBy": "original", "tmdbId": 1241, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "COLLECTION"},
                    {"title": "Fantastic Beasts Collection", "sortBy": "original", "tmdbId": 435259, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "COLLECTION"},
                    {"title": "Wizarding World", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 84}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{K_IMG}/Franchises/Harry%20Potter/logo.webp",
                "coverImageUrl": f"{K_IMG}/Franchises/Harry%20Potter/cover.webp",
                "heroBackdropUrl": f"{K_IMG}/Franchises/Harry%20Potter/backdrop.webp",
                "catalogSources": [],
                "focusGifEnabled": False
            },
            {
                "id": "folder-ultimate-sagas",
                "title": "The Ultimate Sagas & Marathons",
                "sources": [
                    {"title": "Dune Collection", "sortBy": "original", "tmdbId": 726871, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "COLLECTION"},
                    {"title": "The Fast and the Furious", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 75},
                    {"title": "John Wick", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 2435487},
                    {"title": "The MonsterVerse", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 25439284},
                    {"title": "Mission: Impossible", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 131},
                    {"title": "Jurassic Park/World", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 29684902},
                    {"title": "Transformers", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 176},
                    {"title": "The Matrix", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 81},
                    {"title": "Avatar", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 884},
                    {"title": "Pirates of the Caribbean", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 10},
                    {"title": "Jason Bourne", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 36572888},
                    {"title": "Top Gun", "sortBy": "released", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 5022174}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{NUVIO_CDN}/Sagas_logo.webp",
                "coverImageUrl": f"{NUVIO_CDN}/Sagas_cover.webp",
                "heroBackdropUrl": f"{NUVIO_CDN}/Sagas_backdrop.webp",
                "catalogSources": [],
                "focusGifEnabled": False
            }
        ],
        "pinToTop": True,
        "viewMode": "TABBED_GRID",
        "showAllTab": True,
        "focusGlowEnabled": True
    }

    # =========================================================================
    # 04 ✦ REGIONAL & SPOTLIGHT (Languages + Seasonal)
    # =========================================================================
    spotlight_collection = {
        "id": "collection-spotlight-regional",
        "title": "Regional & Spotlight",
        "folders": [
            {
                "id": "folder-bollywood-hindi",
                "title": "Bollywood 🇮🇳",
                "sources": [
                    {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": bollywood_movie},
                    {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": bollywood_series},
                    {
                        "title": "Top Bollywood Hits",
                        "sortBy": "popularity.desc",
                        "filters": {"withOriginalLanguage": "hi"},
                        "provider": "tmdb",
                        "mediaType": "MOVIE",
                        "tmdbSourceType": "DISCOVER"
                    }
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{NUVIO_CDN}/Bollywood_logo.webp",
                "coverImageUrl": f"{NUVIO_CDN}/Bollywood_cover.webp",
                "heroBackdropUrl": f"{NUVIO_CDN}/Bollywood_backdrop.webp",
                "catalogSources": [
                    {"type": "movie", "addonId": addon_id, "catalogId": bollywood_movie},
                    {"type": "series", "addonId": addon_id, "catalogId": bollywood_series}
                ],
                "focusGifEnabled": False
            },
            {
                "id": "folder-south-indian",
                "title": "South Indian Cinema",
                "sources": [
                    {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": south_indian_movie},
                    {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": south_indian_series},
                    {
                        "title": "South Indian Blockbusters",
                        "sortBy": "popularity.desc",
                        "filters": {"withOriginalLanguage": "te|ta|ml|kn"},
                        "provider": "tmdb",
                        "mediaType": "MOVIE",
                        "tmdbSourceType": "DISCOVER"
                    }
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{NUVIO_CDN}/SouthIndian_logo.webp",
                "coverImageUrl": f"{NUVIO_CDN}/SouthIndian_cover.webp",
                "heroBackdropUrl": f"{NUVIO_CDN}/SouthIndian_backdrop.webp",
                "catalogSources": [
                    {"type": "movie", "addonId": addon_id, "catalogId": south_indian_movie},
                    {"type": "series", "addonId": addon_id, "catalogId": south_indian_series}
                ],
                "focusGifEnabled": False
            },
            {
                "id": "folder-anime-spotlight",
                "title": "Anime Spotlight 🇯🇵",
                "sources": [
                    {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": anime_series},
                    {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": anime_movie},
                    {
                        "title": "Trending Anime Series",
                        "sortBy": "popularity.desc",
                        "filters": {
                            "withGenres": "16",
                            "voteCountGte": 60,
                            "withoutKeywords": "198385|378816|155477|256466|329280|570|1817|15078|13059|195669|325693|360629|284535|356759|337946",
                            "withOriginalLanguage": "ja"
                        },
                        "provider": "tmdb",
                        "mediaType": "TV",
                        "tmdbSourceType": "DISCOVER"
                    },
                    {
                        "title": "Trending Anime Films",
                        "sortBy": "popularity.desc",
                        "filters": {
                            "withGenres": "16",
                            "voteCountGte": 120,
                            "withoutKeywords": "198385|378816|155477|256466|329280|570|1817|15078|13059|195669|325693|360629|284535|356759|337946",
                            "withOriginalLanguage": "ja"
                        },
                        "provider": "tmdb",
                        "mediaType": "MOVIE",
                        "tmdbSourceType": "DISCOVER"
                    }
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{K_IMG}/Anime/Popular/logo.webp",
                "coverImageUrl": f"{K_IMG}/Anime/Popular/cover.webp",
                "heroBackdropUrl": f"{K_IMG}/Anime/Popular/backdrop.webp",
                "catalogSources": [
                    {"type": "series", "addonId": addon_id, "catalogId": anime_series},
                    {"type": "movie", "addonId": addon_id, "catalogId": anime_movie}
                ],
                "focusGifEnabled": False
            },
            {
                "id": "folder-kdrama-korean",
                "title": "K-Drama & Korean Waves 🇰🇷",
                "sources": [
                    {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": kdrama_series},
                    {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": kdrama_movie},
                    {
                        "title": "Trending K-Dramas",
                        "sortBy": "popularity.desc",
                        "filters": {"withOriginalLanguage": "ko"},
                        "provider": "tmdb",
                        "mediaType": "TV",
                        "tmdbSourceType": "DISCOVER"
                    },
                    {
                        "title": "Top Korean Cinema",
                        "sortBy": "vote_average.desc",
                        "filters": {"withOriginalLanguage": "ko", "voteCountGte": 200},
                        "provider": "tmdb",
                        "mediaType": "MOVIE",
                        "tmdbSourceType": "DISCOVER"
                    }
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{NUVIO_CDN}/KDrama_logo.webp",
                "coverImageUrl": f"{NUVIO_CDN}/KDrama_cover.webp",
                "heroBackdropUrl": f"{NUVIO_CDN}/KDrama_backdrop.webp",
                "catalogSources": [
                    {"type": "series", "addonId": addon_id, "catalogId": kdrama_series},
                    {"type": "movie", "addonId": addon_id, "catalogId": kdrama_movie}
                ],
                "focusGifEnabled": False
            },
            {
                "id": "folder-seasonal-holidays",
                "title": "Seasonal & Holiday Specials 🎃 🎄",
                "sources": [
                    {"title": "Christmas Holiday Collection", "sortBy": "rank", "sortHow": "asc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 20735298},
                    {"title": "Halloween & Spooky Season", "sortBy": "released", "sortHow": "desc", "provider": "trakt", "mediaType": "MOVIE", "traktListId": 3877179},
                    {"title": "Valentine's Day (Romance & Date Night)", "sortBy": "popularity.desc", "filters": {"withGenres": "10749", "voteCountGte": 150}, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "DISCOVER"},
                    {"title": "Summer Blockbusters & Popcorn Hits", "sortBy": "popularity.desc", "filters": {"withGenres": "28|12", "voteCountGte": 300}, "provider": "tmdb", "mediaType": "MOVIE", "tmdbSourceType": "DISCOVER"}
                ],
                "hideTitle": False,
                "tileShape": "landscape",
                "focusGifUrl": "",
                "titleLogoUrl": f"{NUVIO_CDN}/Seasonal_logo.webp",
                "coverImageUrl": f"{NUVIO_CDN}/Seasonal_cover.webp",
                "heroBackdropUrl": f"{NUVIO_CDN}/Seasonal_backdrop.webp",
                "catalogSources": [],
                "focusGifEnabled": False
            }
        ],
        "pinToTop": True,
        "viewMode": "TABBED_GRID",
        "showAllTab": True,
        "focusGlowEnabled": True
    }

    # Sanitize all collections to guarantee Nuvio never encounters a null catalogId:
    def _sanitize_collection(coll: dict) -> dict:
        cleaned_folders = []
        for f in coll.get("folders") or []:
            # Upstream's Nov-11 sanitize dropped folders with zero ADDON
            # sources — which silently killed the TMDB/trakt-only Franchises
            # collection. Fork fix: addon sources need a resolved catalogId;
            # remote (tmdb/trakt) sources are always valid.
            sources = [
                s for s in (f.get("sources") or [])
                if (s.get("provider") == "addon" and s.get("catalogId"))
                or s.get("provider") in ("tmdb", "trakt")
            ]
            if not sources:
                continue
            folder_copy = dict(f)
            folder_copy["sources"] = sources
            folder_copy["catalogSources"] = [
                cs for cs in (f.get("catalogSources") or [])
                if cs.get("catalogId")
            ]
            cleaned_folders.append(folder_copy)
        coll_copy = dict(coll)
        coll_copy["folders"] = cleaned_folders
        return coll_copy

    by_type = {
        "discover": [discover_collection],
        "ott": [ott_collection],
        "franchises": [franchises_collection],
        "spotlight": [spotlight_collection],
    }
    collections = by_type.get(collection_type, [discover_collection, ott_collection, franchises_collection, spotlight_collection])
    return [c for c in map(_sanitize_collection, collections) if c["folders"]]



#----- Dynamic Nuvio Collection JSON for this token
@router.get("/{token}/nuvio-collection.json")
async def get_nuvio_collection(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "full", token_data)


@router.get("/{token}/nuvio-discover-collection.json")
async def get_nuvio_discover_collection(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "discover", token_data)


@router.get("/{token}/nuvio-ott-collection.json")
async def get_nuvio_ott_collection(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "ott", token_data)


@router.get("/{token}/nuvio-explore-collection.json")
@router.get("/{token}/nuvio-franchises-collection.json")
async def get_nuvio_franchises_collection(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "franchises", token_data)


@router.get("/{token}/nuvio-spotlight-collection.json")
async def get_nuvio_spotlight_collection(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "spotlight", token_data)
