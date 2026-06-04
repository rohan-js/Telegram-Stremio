<p align="center">
  <img src="https://iili.io/KhN0ztj.png" alt="Telegram Stremio Logo" width="400"/>
</p>

<p align="center">
  A powerful, self-hosted <b>Telegram Stremio Media Server</b> built with <b>FastAPI</b>, <b>MongoDB</b>, <b>PyroFork</b>, <b>qBittorrent</b>, and <b>Stremio</b>.
</p>

<p align="center">
  Index Telegram files, magnet links, and torrent files, then stream them through a private Stremio addon with admin dashboards, subscriptions, custom catalogs, and repair tools.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/MongoDB-47A248?logo=mongodb&logoColor=white" alt="MongoDB" />
  <img src="https://img.shields.io/badge/PyroFork-EE3A3A?logo=python&logoColor=white" alt="PyroFork" />
  <img src="https://img.shields.io/badge/Stremio-8D3DAF?logo=stremio&logoColor=white" alt="Stremio" />
  <img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/qBittorrent-2F67BA?logo=qbittorrent&logoColor=white" alt="qBittorrent" />
  <img src="https://img.shields.io/badge/Telegram-26A5E4?logo=telegram&logoColor=white" alt="Telegram" />
</p>

---

## 🧭 Quick Navigation

- [🚀 Introduction](#-introduction)
  - [✨ Key Features](#-key-features)
  - [🆕 Current Features](#-current-features)
- [⚙️ How It Works](#️-how-it-works)
  - [Overview](#overview)
  - [Behind The Scenes](#behind-the-scenes)
- [📤 Upload Guidelines](#-upload-guidelines)
  - [Movies](#-movies)
  - [TV Episodes](#-tv-episodes)
  - [Season Packs](#-season-packs)
  - [Magnet Links And Torrent Files](#-magnet-links-and-torrent-files)
- [🌊 Stream Types](#-stream-types)
  - [Telegram Streams](#-telegram-streams)
  - [Native Torrent Streams](#-native-torrent-streams)
  - [Downloaded VPS Streams](#-downloaded-vps-streams)
- [🤖 Bot Commands](#-bot-commands)
- [🎬 Stremio Addon Integration](#-stremio-addon-integration)
- [🧠 Admin Panel](#-admin-panel)
- [📚 Custom Catalogs And Metadata Tools](#-custom-catalogs-and-metadata-tools)
- [💳 Subscription And Access Management](#-subscription-and-access-management)
- [🔧 Configuration Guide](#-configuration-guide)
  - [Startup And Telegram](#-startup-and-telegram)
  - [Streaming And Routing](#-streaming-and-routing)
  - [Torrents And Downloads](#-torrents-and-downloads)
  - [Admin, Subscription, And Proxy](#-admin-subscription-and-proxy)
- [🚀 Deployment Guide](#-deployment-guide)
  - [Recommended Prerequisites](#-recommended-prerequisites)
  - [Docker Compose VPS Setup](#-docker-compose-vps-setup)
  - [qBittorrent Notes](#-qbittorrent-notes)
  - [Domain And HTTPS](#-domain-and-https)
- [🩺 Operations And Health Checks](#-operations-and-health-checks)
- [🛠️ Troubleshooting](#️-troubleshooting)
- [🔐 Security Notes](#-security-notes)
- [🏅 Credits / License](#-credits--license)

---

# 🚀 Introduction

**Telegram Stremio** turns authorized Telegram channels into a private Stremio media addon. You post video files, magnet links, or `.torrent` files in Telegram, and the bot indexes them into MongoDB with metadata from TMDb/IMDb. Stremio then reads the addon manifest, catalogs, metadata, and streams from the FastAPI server.

This project is designed for personal or private-community use where you control the Telegram channels, the bot, the database, and the VPS.

> Use this project only with media you are allowed to store and stream.

## ✨ Key Features

- 📡 **Telegram file indexing** for movies, TV episodes, and season packs.
- 🎬 **Private Stremio addon** with tokenized manifest, catalog, meta, and stream routes.
- ⚡ **Direct Telegram streaming** through authenticated `/dl/...` HTTP streams.
- 🌊 **Correct media-player support** with `HEAD`, `Range`, `206 Partial Content`, MIME normalization, and seek-friendly chunk sizes.
- 🧲 **Native magnet and torrent support** using Stremio `infoHash`, `fileIdx`, and tracker sources.
- ⬇️ **Download to VPS** option for torrents using qBittorrent and stable `/downloaded/...` playback.
- 🧠 **Smart Telegram routing** with multi-client support, DC awareness, probing, timeout scoring, cooldowns, and fallback retry.
- 🛡️ **Adaptive prefetch** that protects small VPS machines by lowering load when RAM is low or multiple streams are active.
- 🗂️ **Custom catalogs and auto catalogs** for curated and generated Stremio sections.
- 🛠️ **Metadata repair tools** including unmatched media, metadata rescan, and manual IMDb/TMDb correction flows.
- 🧹 **Manual duplicate controls** to hide, show, recommend, and annotate stream qualities without deleting them.
- 💳 **Subscription management** with plans, payment review, token generation, expiry handling, and access controls.
- 📊 **Admin dashboards** for active streams, failed streams, egress, token usage, Telegram routing, and watch-request tracking.

## 🆕 Current Features

- ✅ **Callback-based Watch in Stremio tracking** - channel buttons record who requested a watch link.
- ✅ **Watch Requests page** - a dedicated admin view for recent Telegram watch-link clicks.
- ✅ **Unmatched media page** - repair files or torrents that failed metadata detection.
- ✅ **Metadata rescan UI** - replace incorrect metadata while preserving all stream qualities.
- ✅ **Custom catalog manager** - create, edit, hide, delete, and fill custom catalogs.
- ✅ **Auto catalog generation** - opt-in background/manual generation for language, OTT, and smart categories.
- ✅ **Native torrent streams** - Stremio can play torrents directly without the VPS proxying the video.
- ✅ **qBittorrent download jobs** - download selected torrents to VPS storage for stable HTTP playback.
- ✅ **Stream analytics** - active sessions, TTFB, ranges, chunk size, fallbacks, cooldowns, and recent errors.
- ✅ **VPS protection defaults** - adaptive prefetch, memory guardrails, and watchdog-friendly health routes.

---

# ⚙️ How It Works

## Overview

When you post media in an authorized Telegram channel, the bot queues the item, reads its filename or caption, detects metadata, and stores a stream quality in MongoDB. The FastAPI server exposes that information to Stremio.

```text
Telegram channel
  -> Bot ingestion queue
  -> Metadata detection
  -> MongoDB
  -> FastAPI Stremio addon
  -> Stremio client
```

For Telegram files, the VPS does not need to pre-download the whole video. It fetches Telegram byte ranges on demand and serves them as HTTP video responses.

```text
Stremio player
  -> /dl/... Range request
  -> FastAPI stream route
  -> PyroFork Telegram media session
  -> Telegram byte chunks
  -> Stremio playback
```

For magnet links and `.torrent` files, the addon can return native Stremio torrent streams. In that mode, Stremio handles torrent playback directly.

```text
Magnet or .torrent
  -> infoHash + fileIdx
  -> Stremio native torrent stream
  -> Stremio client handles peers
```

## Behind The Scenes

| Component | Role |
| :--- | :--- |
| **Telegram Bot** | Watches authorized channels, indexes uploads, handles callbacks, and sends admin replies. |
| **PyroFork** | Opens Telegram media sessions and fetches file byte ranges. |
| **FastAPI** | Hosts admin UI, Stremio addon routes, `/dl/...`, `/downloaded/...`, and analytics routes. |
| **MongoDB** | Stores metadata, stream qualities, subscriptions, tokens, catalogs, jobs, watch requests, and analytics. |
| **Stremio** | Reads manifest, catalog, meta, and stream responses from the addon. |
| **qBittorrent** | Optional internal downloader for torrent download-to-VPS jobs. |
| **Nginx** | Optional HTTPS reverse proxy and optional internal file offload for cached/downloaded files. |

---

# 📤 Upload Guidelines

Post media only in channels or groups listed in `AUTH_CHANNEL`. The bot should be an admin in those channels.

## 🎥 Movies

Use a filename or caption with the movie title, year, and quality.

```text
Example Movie 2025 1080p WEB-DL x265.mkv
```

Recommended signals:

- 🎞️ Title
- 📅 Year
- 📺 Resolution such as `720p`, `1080p`, or `2160p`
- 🎧 Audio/source/codec tags such as `WEB-DL`, `BluRay`, `x265`, `10bit`, `DDP5.1`

## 📺 TV Episodes

Use standard season and episode patterns.

```text
Example.Show.S01E04.1080p.WEB-DL.x265.mkv
```

Recommended signals:

- 🎞️ Show title
- 📆 Season number such as `S01`
- 🎬 Episode number such as `E04`
- 📺 Resolution and source tags

## 📦 Season Packs

Full-season packs should include a clear season signal.

```text
Example Show S01 COMBINED 1080p WEB-DL.mkv
```

For multi-file torrents, uploaded `.torrent` files are better than plain magnets because they expose the internal file list. That lets the addon map each episode to the correct Stremio `fileIdx`.

## 🧲 Magnet Links And Torrent Files

Supported inputs:

- Magnet links in channel text.
- Magnet links in captions.
- Uploaded `.torrent` documents.

Behavior:

- Magnets are indexed as native torrent streams.
- `.torrent` files are parsed for info hash, trackers, file names, file sizes, and file indexes.
- Multi-file torrents can map TV episodes to exact files.
- If metadata cannot be inferred, the item appears in the unmatched media repair flow.

## 🏷️ Metadata Repair

If the title is matched incorrectly or metadata detection fails:

1. Open the admin UI.
2. Go to **Unmatched** for failed items, or **Media** for already indexed items.
3. Search for the correct TMDb/IMDb match.
4. Apply the match.

The repair flow preserves existing stream qualities, including Telegram file IDs, torrent hashes, file indexes, downloaded torrent linkage, notes, hidden flags, and recommended flags.

---

# 🌊 Stream Types

The same movie or episode can expose multiple stream types. You can keep all of them and manually hide or recommend specific qualities from the admin UI.

| Stream Type | Best For | How It Plays |
| :--- | :--- | :--- |
| **Telegram Stream** | Fast indexing without local storage. | VPS fetches Telegram byte ranges and serves `/dl/...`. |
| **Native Torrent Stream** | Letting Stremio handle peers directly. | Addon returns `infoHash`, optional `fileIdx`, and trackers. |
| **Downloaded VPS Stream** | Stable playback after a torrent is downloaded. | qBittorrent downloads the file; VPS serves `/downloaded/...`. |
| **VLC / External Player** | Difficult MKV or device-specific seek issues. | Use the external player from Stremio or Telegram link workflows. |

## 📡 Telegram Streams

Telegram streams are served by the VPS through authenticated `/dl/...` URLs.

Current streaming behavior:

- `HEAD` responses include stable `Content-Length`.
- `Range` requests return `206 Partial Content`.
- `Accept-Ranges: bytes` is returned for media players.
- Known video extensions get stable video MIME types.
- Range/seek requests use `512 KB` Telegram chunks for faster seek start.
- Normal full-file streams use `1 MB` Telegram chunks for better throughput.
- Failed Telegram chunks retry through another healthy client before failing cleanly.
- The streamer does **not** zero-pad failed chunks.

### ⚙️ `PARALLEL` And `PRE_FETCH`

These two settings control Telegram chunk fetching:

| Variable | Meaning |
| :--- | :--- |
| `PARALLEL` | Maximum concurrent Telegram chunk fetches per stream. |
| `PRE_FETCH` | Number of chunks queued ahead of playback. |

Example:

```env
PARALLEL="3"
PRE_FETCH="3"
ADAPTIVE_PREFETCH_ENABLED="true"
```

Higher `PARALLEL` can improve burst speed, but it increases Telegram API pressure, memory usage, and risk on small VPS machines. Higher `PRE_FETCH` can improve buffering, but it also holds more data in memory.

Adaptive prefetch never raises these values. It only lowers them when the VPS is busy, RAM is low, the file/request is small, or multiple streams are active.

## 🧲 Native Torrent Streams

For magnets and `.torrent` files, the addon can return native Stremio streams:

```json
{
  "infoHash": "example_info_hash",
  "fileIdx": 0,
  "sources": ["tracker:udp://tracker.example.org:80/announce"]
}
```

In native torrent mode:

- The VPS does not download or proxy video bytes.
- Stremio handles peers and playback.
- Stremio Desktop or Stremio Service may be required for some clients.
- Playback speed depends on seeders, peers, trackers, and the Stremio device.

## ⬇️ Downloaded VPS Streams

If torrent playback is weak or you want stable HTTP playback, use **Download to VPS**.

Flow:

1. Post a magnet or `.torrent`.
2. The bot indexes the torrent.
3. Click **Download to VPS**.
4. qBittorrent downloads to persistent storage.
5. The addon exposes a `/downloaded/...` stream.

qBittorrent WebUI should stay internal. Do not expose it publicly.

---

# 🤖 Bot Commands

| Command | Description |
| :--- | :--- |
| `/start` | Sends the user addon link or subscription/access information. |
| `/log` | Sends recent logs to the owner/admin when enabled by the bot. |
| `/set` | Helps manually attach metadata or configure a file flow depending on current bot behavior. |

Telegram channel replies can include:

- ▶️ **Watch in Stremio** callback button.
- 🎬 A follow-up Stremio watch link in the channel after a user taps the callback.
- 📥 **Download to VPS** for eligible torrent streams.
- Streaming notes for WARP, issue reporting, and device-specific seek behavior.

---

# 🎬 Stremio Addon Integration

Each user or mode gets an addon URL like:

```text
https://your-domain.example/stremio/YOUR_TOKEN/manifest.json
```

Install flow:

1. Start the Telegram bot with `/start`.
2. Copy the Stremio addon URL.
3. Open Stremio.
4. Install the addon using the manifest URL.
5. Browse catalogs or search media.

Important addon routes:

| Route | Purpose |
| :--- | :--- |
| `/stremio/{token}/manifest.json` | Tokenized addon manifest. |
| `/stremio/{token}/catalog/{type}/{id}.json` | Catalog responses. |
| `/stremio/{token}/meta/{type}/{id}.json` | Metadata responses. |
| `/stremio/{token}/stream/{type}/{id}.json` | Stream responses. |
| `/dl/...` | Telegram-backed HTTP stream. |
| `/downloaded/...` | Downloaded torrent HTTP stream. |

If subscriptions are enabled, token access and expiry are enforced by the addon.

---

# 🧠 Admin Panel

Protected admin pages:

| Page | Purpose |
| :--- | :--- |
| `/dashboard` | Main system overview, active streams, failures, egress, and token usage. |
| `/admin/dashboard` | Admin system stats and operational tools. |
| `/media/manage` | Browse movies and series. |
| `/media/edit` | Edit metadata and manage stream qualities. |
| `/catalogs` | Create custom catalogs and manage auto catalogs. |
| `/unmatched` | Repair failed metadata matches. |
| `/watch-requests` | See Telegram users who clicked Watch in Stremio callbacks. |
| `/admin/subscriptions` | Manage subscription plans. |
| `/admin/access` | View, extend, revoke, and reassign access. |
| `/status` | Public status page. |

The stream dashboard can show:

- Active stream ID and source type.
- File name and request range.
- Client IP and user-agent.
- Telegram client/DC routing details.
- Chunk size, TTFB, fallback count, timeout count, and cooldown state.
- Recent failed streams and error reasons.
- Recent watch-link requesters from Telegram callback clicks.

---

# 📚 Custom Catalogs And Metadata Tools

## 🗂️ Custom Catalogs

Custom catalogs let admins curate Stremio sections manually.

You can:

- Create movie or series collections.
- Add and remove media.
- Hide or show catalogs.
- Search existing indexed media.
- Keep visible catalogs in the Stremio manifest.

## 🤖 Auto Catalogs

Auto catalogs are opt-in from the admin UI. They can classify indexed media into language, OTT, and smart categories such as recently added or top rated.

No heavy full rebuild is required on first boot. Admins choose enabled auto-catalog options from `/catalogs`.

## 🛠️ Unmatched Media

The unmatched media page stores failed ingestion references, not video bytes.

Use it when:

- A filename is unclear.
- A torrent has no obvious title.
- A season pack cannot map correctly.
- Metadata search returned the wrong result.

## 🔁 Metadata Rescan

Metadata rescan updates title, year, poster, backdrop, overview, genres, cast, rating, runtime, IDs, and season/episode metadata. It preserves all stream quality entries.

## 🧹 Duplicate And Quality Controls

Admins can manually:

- Hide a stream from Stremio.
- Show a hidden stream again.
- Mark a stream as recommended.
- Add a quality note such as `bad seek`, `slow`, `duplicate`, or `TV risky`.

The app detects duplicates but does not auto-delete or auto-hide them.

---

# 💳 Subscription And Access Management

Subscription mode can restrict addon access to approved users.

Supported admin flows:

- Create and edit subscription plans.
- Review payment requests.
- Generate user access tokens.
- Extend, reduce, revoke, or reassign access.
- Show expired users a clear expired stream response.

When `SUBSCRIPTION` is disabled, the addon can use `DEFAULT_ADDON_TOKEN` for shared/default access.

---

# 🔧 Configuration Guide

Copy the sample file and edit it:

```bash
cp sample_config.env config.env
nano config.env
```

Never commit real secrets.

## 🧩 Startup And Telegram

| Variable | Description |
| :--- | :--- |
| `API_ID` | Telegram API ID from `my.telegram.org`. |
| `API_HASH` | Telegram API hash from `my.telegram.org`. |
| `BOT_TOKEN` | Main bot token from BotFather. |
| `HELPER_BOT_TOKEN` | Optional helper bot token. |
| `OWNER_ID` | Telegram owner/admin user ID. |
| `AUTH_CHANNEL` | Comma-separated authorized Telegram channel/group IDs. |
| `DATABASE` | MongoDB connection URI or comma-separated URIs. |
| `TMDB_API` | TMDb API key/token for metadata lookup. |
| `BASE_URL` | Public HTTPS base URL, without trailing slash. |
| `PORT` | FastAPI port inside the container. |
| `DEFAULT_ADDON_TOKEN` | Shared addon token used for default/free access flows. |
| `MULTI_TOKEN1`, `MULTI_TOKEN2`, ... | Additional Telegram bot tokens for multi-client fetching. |

## 🌊 Streaming And Routing

| Variable | Description |
| :--- | :--- |
| `PARALLEL` | Maximum concurrent Telegram chunk fetches per stream. |
| `PRE_FETCH` | Number of chunks queued ahead of playback. |
| `SMART_ROUTING_ENABLED` | Enable smart client/DC route selection. |
| `SMART_ROUTING_PROBE_ENABLED` | Probe candidate clients before selecting a route. |
| `SMART_ROUTING_PROBE_CLIENTS` | Number of candidate clients to probe. |
| `SMART_ROUTING_PROBE_BYTES` | Bytes to fetch during a probe. |
| `SMART_ROUTING_PROBE_TIMEOUT_SEC` | Timeout for probe requests. |
| `SMART_ROUTING_FIRST_CHUNK_TIMEOUT_SEC` | Timeout for the first stream chunk. |
| `SMART_ROUTING_CHUNK_TIMEOUT_SEC` | Timeout for normal chunks. |
| `SMART_ROUTING_COOLDOWN_FAILURES` | Failures before a client/DC is put on cooldown. |
| `SMART_ROUTING_COOLDOWN_SEC` | Cooldown duration in seconds. |
| `ADAPTIVE_PREFETCH_ENABLED` | Let the app lower prefetch/parallelism for safety. |
| `ADAPTIVE_PREFETCH_LOW_MEM_MB` | Force safer `1/1` behavior below this free-memory threshold. |
| `ADAPTIVE_PREFETCH_MULTI_STREAM_THRESHOLD` | Reduce values when active streams reach this count. |
| `ADAPTIVE_PREFETCH_SMALL_REQUEST_BYTES` | Reduce for small range requests. |
| `ADAPTIVE_PREFETCH_SMALL_FILE_BYTES` | Reduce for small files. |
| `STREAM_SLO_TTFB_WARN_SEC` | Log slow time-to-first-byte warnings. |
| `STREAM_SLO_TIMEOUT_WARN_COUNT` | Log warning after repeated chunk timeouts. |
| `STREAM_SLO_BUFFERING_WARN_RATE` | Log possible buffering warnings by stream ratio. |

Recommended small-VPS baseline:

```env
PARALLEL="3"
PRE_FETCH="3"
ADAPTIVE_PREFETCH_ENABLED="true"
```

## 💾 Disk Cache And Nginx Offload

| Variable | Description |
| :--- | :--- |
| `DISK_CACHE_ENABLED` | Enable optional Telegram file disk caching. |
| `DISK_CACHE_PRECACHE_ON_INGEST` | Pre-cache Telegram files during ingestion. |
| `DISK_CACHE_DIR` | Cache directory inside the app container. |
| `DISK_CACHE_MAX_GB` | Cache size limit in GB. |
| `DISK_CACHE_MAX_BYTES` | Cache size limit in bytes. |
| `DISK_CACHE_CONCURRENCY` | Cache worker concurrency. |
| `NGINX_ACCEL_REDIRECT_ENABLED` | Use Nginx internal redirect for cached Telegram files. |
| `NGINX_ACCEL_REDIRECT_LOCATION` | Internal Nginx cache location. |

## 🧲 Torrents And Downloads

| Variable | Description |
| :--- | :--- |
| `TORRENT_STATS_ENABLED` | Scrape tracker seed/peer estimates for torrent streams. |
| `TORRENT_STATS_TTL_SEC` | Successful torrent stats cache TTL. |
| `TORRENT_STATS_FAILURE_TTL_SEC` | Failed torrent stats cache TTL. |
| `TORRENT_STATS_MAX_TRACKERS` | Maximum trackers to scrape per torrent. |
| `TORRENT_STATS_TIMEOUT_SEC` | Tracker scrape timeout. |
| `TORRENT_STATS_CONCURRENCY` | Tracker scrape concurrency. |
| `TORRENT_DOWNLOADS_ENABLED` | Enable Download to VPS buttons/jobs. |
| `TORRENT_DOWNLOAD_ROOT` | Completed torrent path visible to the app. |
| `TORRENT_DOWNLOAD_MIN_FREE_GB` | Minimum free disk required before accepting downloads. |
| `TORRENT_DOWNLOAD_CONCURRENCY` | Number of active download workers. |
| `TORRENT_DOWNLOAD_POLL_SEC` | qBittorrent polling interval. |
| `TORRENT_DOWNLOAD_PROGRESS_EDIT_SEC` | Minimum seconds between progress edits. |
| `TORRENT_DOWNLOAD_STALL_TIMEOUT_SEC` | Mark stalled downloads failed after this duration. |
| `TORRENT_DOWNLOAD_MAX_RUNTIME_SEC` | Maximum runtime per download job. |
| `QBITTORRENT_BASE_URL` | Internal qBittorrent WebUI URL, usually `http://qbittorrent:8080`. |
| `QBITTORRENT_USERNAME` | Internal qBittorrent username. |
| `QBITTORRENT_PASSWORD` | Internal qBittorrent password. |
| `QBITTORRENT_SAVE_PATH` | Completed path from qBittorrent's view. |
| `QBITTORRENT_TEMP_PATH` | Incomplete path from qBittorrent's view. |
| `NGINX_DOWNLOAD_ACCEL_REDIRECT_ENABLED` | Use Nginx internal redirect for downloaded torrent files. |
| `NGINX_DOWNLOAD_ACCEL_REDIRECT_LOCATION` | Internal Nginx location for downloaded files. |

## 📊 Dashboard Egress And VPS Outbound

| Variable | Description |
| :--- | :--- |
| `NGINX_EGRESS_ENABLED` | Parse Nginx logs for bytes served to clients. |
| `NGINX_EGRESS_LOG_PATHS` | Comma-separated mounted Nginx access log paths. |
| `NGINX_EGRESS_STREAM_PREFIXES` | Path prefixes counted as stream egress. |
| `NGINX_EGRESS_CACHE_SEC` | Egress summary cache TTL. |
| `VPS_OUTBOUND_ENABLED` | Track host network transmit bytes. |
| `VPS_OUTBOUND_INTERFACE` | Host network interface name. |
| `VPS_OUTBOUND_TX_BYTES_PATH` | Mounted sysfs TX bytes path. |
| `VPS_OUTBOUND_NET_DEV_PATH` | Mounted `/proc/net/dev` path. |
| `VPS_OUTBOUND_MONTHLY_LIMIT_BYTES` | Dashboard monthly quota reference. |

## 🔐 Admin, Subscription, And Proxy

| Variable | Description |
| :--- | :--- |
| `ADMIN_USERNAME` | Admin web login username. |
| `ADMIN_PASSWORD` | Admin web login password. |
| `REPLACE_MODE` | Replace same-quality Telegram entries when enabled. |
| `HIDE_CATALOG` | Hide catalog entries globally when enabled. |
| `SUBSCRIPTION` | Enable subscription-gated addon access. |
| `SUBSCRIPTION_GROUP_ID` | Private group/channel ID for subscriber access. |
| `APPROVER_IDS` | Comma-separated Telegram admin IDs for payment approval. |
| `SUBSCRIPTION_URL` | Bot, payment, or contact URL shown to users. |
| `Proxy` | Enable proxy stream entries. |
| `ProxyType` | Proxy type label. |
| `HTTP_Proxy_URL` | Proxy prefix such as a worker URL. |
| `SHOW_ProxyAndNonProxyBoth` | Show both proxied and direct stream links. |
| `UPSTREAM_REPO` | Optional repository URL for update workflows. |
| `UPSTREAM_BRANCH` | Optional branch name for update workflows. |

---

# 🚀 Deployment Guide

Docker Compose on a VPS is the recommended deployment path.

Heroku-style deployments are not recommended for the current feature set because Telegram streaming, qBittorrent downloads, persistent storage, dashboards, and background jobs need a stable long-running host.

## ✅ Recommended Prerequisites

- Linux VPS with Docker Engine and Docker Compose v2.
- Public HTTPS domain for `BASE_URL`.
- Telegram bot token from BotFather.
- Telegram API ID/hash from `my.telegram.org`.
- MongoDB database.
- TMDb API key/token.
- Authorized Telegram channel/group where the bot is admin.
- Optional persistent disk for qBittorrent downloads.

Oracle Always Free A1 Flex is a good free-tier target when capacity is available. E2 Micro can work for light usage, but RAM is tight, so adaptive prefetch should stay enabled.

## 🐳 Docker Compose VPS Setup

```bash
git clone https://github.com/rohan-js/Telegram-Stremio.git
cd Telegram-Stremio
cp sample_config.env config.env
nano config.env
docker compose up -d --build
```

Useful commands:

```bash
docker compose ps
docker logs --tail=100 tg_stremio
docker compose restart telegram-stremio
```

If your host still has legacy Compose, prefer installing the modern Docker Compose plugin. Use `docker-compose` only as a fallback.

## 🧲 qBittorrent Notes

The included Compose setup runs:

| Service | Container | Purpose |
| :--- | :--- | :--- |
| `telegram-stremio` | `tg_stremio` | Main bot, FastAPI app, addon, and stream server. |
| `qbittorrent` | `qbittorrent` | Internal torrent download worker. |

Only expose the torrent peer port when needed:

```text
6881/tcp
6881/udp
```

Do **not** expose qBittorrent WebUI publicly. Keep it internal at:

```text
http://qbittorrent:8080
```

## 🌐 Domain And HTTPS

Set `BASE_URL` to your public HTTPS domain:

```env
BASE_URL="https://your-domain.example"
```

Recommended reverse proxy behavior:

- Terminate HTTPS at Nginx, Caddy, or another reverse proxy.
- Proxy app traffic to the internal FastAPI port.
- Disable buffering for streaming routes.
- Use long read/send timeouts for media streams.
- Optionally use internal redirect locations for cached or downloaded files.

---

# 🩺 Operations And Health Checks

## ✅ Health Checks

| Check | Expected |
| :--- | :--- |
| `/login` | Returns admin login page. |
| `/stream/stats` | Returns stream/session analytics. |
| `/stremio/{token}/manifest.json` | Returns tokenized addon manifest. |
| `docker compose ps` | `telegram-stremio` and `qbittorrent` are running. |

Example:

```bash
curl -I https://your-domain.example/login
curl https://your-domain.example/stream/stats
```

## 🔄 Safe Redeploy

For production, use a restart window when active streams are zero.

```bash
git pull
docker compose build telegram-stremio
curl https://your-domain.example/stream/stats
docker compose up -d telegram-stremio
```

This should restart only the main app container. qBittorrent does not need to restart for normal app updates.

## 📊 Stream Debugging

Use the dashboard and `/stream/stats` to inspect:

- Active stream count.
- Source type.
- File name.
- Request range.
- Telegram client/DC.
- Chunk size.
- TTFB.
- Timeout and fallback counts.
- Cooldown state.
- Error reason.

---

# 🛠️ Troubleshooting

## Metadata Failed

Use clearer filenames with title/year/quality for movies and `SxxEyy` for episodes. For failed items, open `/unmatched`, search the correct metadata, and apply it manually.

## Batch Uploads

Batch uploads are queued and processed one by one. If a file does not index, check `/unmatched` and the bot reply in the Telegram channel.

## Slow Telegram Stream

Try:

- Cloudflare WARP on the viewing device.
- A different Telegram file/release.
- A native torrent stream if the torrent has seeders.
- Download to VPS for stable HTTP playback.
- Lower `PARALLEL`/`PRE_FETCH` if the VPS is under memory pressure.

## Seeking Goes Back To The Beginning On TV/Mobile

Some MKV files or client/player combinations do not seek well, even when Windows Stremio works. Try another release, VLC/external player, a native torrent stream, or a remuxed copy with proper seek cues.

## Native Torrent Does Not Play

Check seeders, trackers, and client support. Stremio Web may require Stremio Desktop or Stremio Service for torrent playback.

## Download To VPS Fails

Check:

- `TORRENT_DOWNLOADS_ENABLED`.
- qBittorrent credentials.
- Free disk space.
- Download paths and container mounts.
- qBittorrent logs.
- Whether the torrent has metadata/seeders.

## Subscription Expired

Check `/admin/access` and `/admin/subscriptions`. Extend the user, approve payment, or disable subscription mode if you want shared/free access.

---

# 🔐 Security Notes

- Never commit `config.env`.
- Never publish real bot tokens, Telegram API hash, MongoDB URI, admin password, private channel IDs, or SSH keys.
- Keep qBittorrent WebUI private.
- Use HTTPS for `BASE_URL`.
- Keep admin pages protected.
- Use this project only with media you are allowed to store and stream.

---

# 🏅 Credits / License

This project is a self-hosted Telegram-to-Stremio media server built around FastAPI, MongoDB, PyroFork, Stremio, Docker, and qBittorrent.

See [LICENSE](LICENSE) before redistribution or public hosting.
