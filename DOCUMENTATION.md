# Telegram-Stremio Documentation

A self-hosted Stremio addon that streams media files directly from Telegram channels. This project bridges Telegram's file storage with Stremio's streaming interface.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [API Reference](#api-reference)
6. [Components](#components)
7. [Database Schema](#database-schema)
8. [Telegram Bot Commands](#telegram-bot-commands)
9. [Deployment](#deployment)
10. [Troubleshooting](#troubleshooting)

---

## Overview

### What is Telegram-Stremio?

Telegram-Stremio is a self-hosted media streaming solution that:

- **Stores media** in Telegram channels (unlimited free storage)
- **Serves as a Stremio addon** providing catalogs and streams
- **Fetches metadata** automatically from TMDB/IMDB
- **Supports multi-audio tracks** with automatic detection
- **Provides an admin panel** for media management

### Key Features

| Feature | Description |
|---------|-------------|
| **Unlimited Storage** | Uses Telegram channels as free cloud storage |
| **Stremio Integration** | Full addon with catalogs, metadata, and streams |
| **Auto Metadata** | Fetches movie/TV info from TMDB and IMDB |
| **Multi-Audio Detection** | FFprobe-based audio track detection |
| **Multi-Client Streaming** | Load balancing across multiple bot clients |
| **Admin Dashboard** | Web UI for managing content |
| **HLS Support** | Experimental adaptive bitrate streaming |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         TELEGRAM                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ Auth Channel│  │ Storage DB 1│  │ Storage DB 2│  ...        │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TELEGRAM-STREMIO SERVER                      │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    Pyrogram Bot Layer                     │  │
│  │  • Main Bot (receives files, commands)                    │  │
│  │  • Multi-Client CDN (load balancing)                      │  │
│  └───────────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    FastAPI Web Layer                      │  │
│  │  • /stremio/* - Stremio addon endpoints                   │  │
│  │  • /dl/*     - Direct download/streaming                  │  │
│  │  • /api/*    - Admin API endpoints                        │  │
│  │  • /admin/*  - Admin dashboard                            │  │
│  └───────────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    Helper Modules                         │  │
│  │  • database.py   - MongoDB operations                     │  │
│  │  • metadata.py   - TMDB/IMDB lookup                       │  │
│  │  • custom_dl.py  - Telegram file streaming                │  │
│  │  • audio_tracks.py - FFprobe audio detection              │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         MONGODB                                 │
│  • telegram_stremio_storage (media documents)                   │
│  • telegram_stremio_tracking (usage analytics)                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                          STREMIO                                │
│  • Displays catalogs from addon                                 │
│  • Streams video via direct download links                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Installation

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- MongoDB Atlas account (free tier works)
- Telegram API credentials (api.telegram.org)
- TMDB API key

### Quick Start with Docker

```bash
# Clone the repository
git clone https://github.com/rohan-js/Telegram-Stremio.git
cd Telegram-Stremio

# Copy and configure environment
cp sample_config.env config.env
# Edit config.env with your credentials

# Run with Docker Compose
docker-compose up -d --build
```

### Manual Installation

```bash
# Install uv (Python package manager)
pip install uv

# Install dependencies
uv sync

# Run the application
python -m Backend
```

---

## Configuration

### Environment Variables

Create a `config.env` file with the following variables:

#### Required

| Variable | Description | Example |
|----------|-------------|---------|
| `API_ID` | Telegram API ID from my.telegram.org | `12345678` |
| `API_HASH` | Telegram API Hash | `abcdef1234567890` |
| `BOT_TOKEN` | Main bot token from @BotFather | `123456:ABC-DEF...` |
| `AUTH_CHANNEL` | Channel ID(s) for receiving files | `-1001234567890` |
| `DATABASE` | MongoDB connection string(s) | `mongodb+srv://...` |
| `BASE_URL` | Public URL for the server | `https://example.com` |
| `TMDB_API` | TMDB API key | `abc123...` |
| `OWNER_ID` | Telegram user ID of admin | `123456789` |

#### Optional

| Variable | Description | Default |
|----------|-------------|---------|
| `HELPER_BOT_TOKEN` | Secondary bot for metadata | Same as `BOT_TOKEN` |
| `PORT` | Server port | `8000` |
| `PARALLEL` | Parallel chunk downloads | `1` |
| `PRE_FETCH` | Prefetch chunk count | `1` |
| `REPLACE_MODE` | Replace duplicate entries | `true` |
| `HIDE_CATALOG` | Hide catalog from Stremio | `false` |
| `ADMIN_USERNAME` | Admin panel username | `fyvio` |
| `ADMIN_PASSWORD` | Admin panel password | `fyvio` |

#### Multi-Client CDN (Optional)

Add additional bot tokens for load balancing:

```env
MULTI_TOKEN1 = "bot_token_1"
MULTI_TOKEN2 = "bot_token_2"
MULTI_TOKEN3 = "bot_token_3"
```

---

## API Reference

### Stremio Addon Endpoints

Base path: `/stremio`

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/manifest.json` | GET | Addon manifest with capabilities |
| `/catalog/{type}/{id}.json` | GET | Movie/TV catalog with pagination |
| `/meta/{type}/{id}.json` | GET | Metadata for specific title |
| `/stream/{type}/{id}.json` | GET | Available streams for title |

#### Example: Get Manifest
```bash
curl https://your-server.com/stremio/manifest.json
```

#### Example: Get Movie Streams
```bash
curl https://your-server.com/stremio/stream/movie/tt1234567.json
```

### Streaming Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/dl/{id}/{name}` | GET/HEAD | Direct video stream |
| `/stream/stats` | GET | Active streaming statistics |
| `/stream/{stream_id}` | GET | Specific stream details |
| `/probe/audio/{id}` | GET | Probe audio tracks in file |

### HLS Streaming (Experimental)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/hls/{id}/master.m3u8` | GET | HLS master playlist |
| `/hls/{id}/{quality}/playlist.m3u8` | GET | Quality-specific playlist |
| `/hls/{id}/{quality}/segment_{num}.ts` | GET | Video segment |

### Admin API Endpoints

Base path: `/api`

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/list` | GET | List media with pagination |
| `/delete` | DELETE | Delete media entry |
| `/update` | PUT | Update media metadata |
| `/details` | GET | Get full media details |
| `/movie/quality/delete` | DELETE | Delete specific quality |
| `/tv/quality/delete` | DELETE | Delete TV episode quality |
| `/tv/episode/delete` | DELETE | Delete entire episode |
| `/tv/season/delete` | DELETE | Delete entire season |

---

## Components

### Backend Structure

```
Backend/
├── __init__.py          # Version and DB initialization
├── __main__.py          # Application entry point
├── config.py            # Environment configuration
├── logger.py            # Logging setup with IST timezone
├── fastapi/             # Web server
│   ├── app.py           # FastAPI application
│   └── routes/
│       ├── stremio_routes.py   # Stremio addon endpoints
│       ├── stream_routes.py    # Video streaming + HLS
│       ├── api_routes.py       # Admin management API
│       └── template_routes.py  # Admin dashboard UI
├── helper/              # Core modules
│   ├── database.py      # MongoDB operations
│   ├── metadata.py      # TMDB/IMDB fetching
│   ├── custom_dl.py     # Telegram file streaming
│   ├── audio_tracks.py  # FFprobe audio detection
│   ├── hls_transcoder.py # HLS segment generation
│   ├── encrypt.py       # URL encoding/decoding
│   ├── imdb.py          # IMDB API wrapper
│   ├── modal.py         # Pydantic schemas
│   ├── pyro.py          # Pyrogram utilities
│   └── task_manager.py  # Async task management
└── pyrofork/            # Telegram bot
    ├── bot.py           # Bot initialization
    └── plugins/
        ├── reciever.py  # File upload handler
        ├── start.py     # /start command
        ├── restart.py   # /restart command
        ├── announce.py  # /announce command
        ├── manual.py    # /manual command
        ├── fix_metadata.py # Metadata repair tools
        └── log.py       # /log command
```

### Key Modules

#### database.py
Handles all MongoDB operations:
- Multi-database support for sharding
- Movie and TV show CRUD operations
- Quality/episode management
- Search functionality

#### metadata.py
Fetches metadata from external APIs:
- PTN (Parse Torrent Name) for filename parsing
- TMDB API for movie/TV details
- IMDB fallback for additional info
- Automatic year and quality detection

#### custom_dl.py
Telegram file streaming engine:
- ByteStreamer class for chunk-based streaming
- Multi-client load balancing
- DC (Data Center) optimization
- Prefetch and parallel download support

#### audio_tracks.py
Audio track detection:
- FFprobe-based analysis
- Language detection
- Codec and channel info
- Caching for performance

---

## Database Schema

### Movie Document

```json
{
  "_id": "ObjectId",
  "tmdb_id": 12345,
  "imdb_id": "tt1234567",
  "db_index": 1,
  "title": "Movie Title",
  "genres": ["Action", "Adventure"],
  "description": "Movie description...",
  "rating": 7.5,
  "release_year": 2024,
  "poster": "https://image.tmdb.org/...",
  "backdrop": "https://image.tmdb.org/...",
  "logo": "https://image.tmdb.org/...",
  "cast": ["Actor 1", "Actor 2"],
  "runtime": "120",
  "media_type": "movie",
  "updated_on": "2024-01-01T00:00:00Z",
  "telegram": [
    {
      "quality": "1080p",
      "id": "encoded_file_id",
      "name": "Movie.2024.1080p.WEB-DL.mkv",
      "size": "2.5GB",
      "audio_tracks": [
        {
          "index": 1,
          "language": "eng",
          "language_name": "English",
          "codec": "AAC",
          "channels": 6,
          "channel_str": "5.1"
        }
      ]
    }
  ]
}
```

### TV Show Document

```json
{
  "_id": "ObjectId",
  "tmdb_id": 67890,
  "imdb_id": "tt9876543",
  "db_index": 1,
  "title": "TV Show Title",
  "genres": ["Drama"],
  "description": "Show description...",
  "rating": 8.5,
  "release_year": 2023,
  "poster": "https://image.tmdb.org/...",
  "backdrop": "https://image.tmdb.org/...",
  "media_type": "tv",
  "seasons": [
    {
      "season_number": 1,
      "episodes": [
        {
          "episode_number": 1,
          "title": "Pilot",
          "episode_backdrop": "https://...",
          "overview": "Episode description...",
          "released": "2023-01-15",
          "telegram": [
            {
              "quality": "720p",
              "id": "encoded_file_id",
              "name": "Show.S01E01.720p.mkv",
              "size": "500MB",
              "audio_tracks": []
            }
          ]
        }
      ]
    }
  ]
}
```

---

## Telegram Bot Commands

| Command | Description | Access |
|---------|-------------|--------|
| `/start` | Welcome message and info | Everyone |
| `/restart` | Restart the bot | Owner only |
| `/announce <message>` | Broadcast to all users | Owner only |
| `/manual <imdb_id>` | Manually add media by IMDB ID | Owner only |
| `/log` | Get recent logs | Owner only |
| `/fix_metadata` | Repair broken metadata entries | Owner only |

### Uploading Media

1. Add the bot to your AUTH_CHANNEL
2. Forward or upload video files to the channel
3. Bot automatically:
   - Parses filename for title, year, quality
   - Fetches metadata from TMDB/IMDB
   - Probes audio tracks with FFprobe
   - Stores in MongoDB
   - Sends confirmation with stream link

---

## Deployment

### Docker Compose (Recommended)

```yaml
version: '3.8'
services:
  telegram-stremio:
    build: .
    container_name: tg_stremio
    restart: unless-stopped
    ports:
      - "8000:8000"
    env_file:
      - config.env
```

### Oracle Cloud (Free Tier)

1. Create ARM instance (4 OCPU, 24GB RAM free)
2. Open port 8000 in security list
3. Install Docker and Docker Compose
4. Clone repo and configure
5. Use Cloudflare Tunnel or nginx for HTTPS

### Environment Setup

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh

# Install Docker Compose
sudo apt install docker-compose

# Clone and run
git clone https://github.com/rohan-js/Telegram-Stremio.git
cd Telegram-Stremio
cp sample_config.env config.env
# Edit config.env
docker-compose up -d --build
```

---

## Troubleshooting

### Common Issues

#### FloodWait Error
```
FloodWait: A wait of X seconds is required
```
**Cause:** Too many Telegram API calls or restarts
**Solution:** Wait for the specified duration, reduce restart frequency

#### Metadata Not Found
**Cause:** TMDB doesn't have the movie/show
**Solution:** Use `/manual <imdb_id>` command or edit via admin panel

#### Streams Not Playing
**Cause:** Invalid file ID or streaming issues
**Solution:** Check logs, verify file exists in channel

#### Container Keeps Restarting
**Cause:** Configuration error or dependency issue
**Solution:** Check logs with `docker logs tg_stremio`

### Useful Commands

```bash
# View logs
docker logs tg_stremio -f

# Restart container
docker-compose restart

# Rebuild after code changes
docker-compose up -d --build

# Check container status
docker ps

# Access container shell
docker exec -it tg_stremio bash
```

### Health Checks

- **API Status:** `https://your-server.com/stremio/manifest.json`
- **Stream Stats:** `https://your-server.com/stream/stats`
- **Admin Panel:** `https://your-server.com/admin`

---

## License

This project is licensed under the GNU General Public License v3.0.

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## Credits

- Original project: [weebzone/Telegram-Stremio](https://github.com/weebzone/Telegram-Stremio)
- Fork maintainer: rohan-js
- Dependencies: Pyrogram, FastAPI, Motor, TMDB API
