MkvSeekWarningText = (
    "⚠️ <b>TV/mobile seek warning:</b> this MKV may restart from the beginning when seeking "
    "because its seek index is only discoverable near the end. If it happens, use Windows/external "
    "player, another release, or a remuxed copy.\n\n"
)


def build_stream_reply_text(metadata_info: dict, size: str, direct_stream: str = "N/A") -> str:
    media_type = metadata_info.get("media_type", "movie")
    movie_title = metadata_info.get("title", "Unknown")
    year = metadata_info.get("year", "")
    quality = metadata_info.get("quality", "")
    rating = metadata_info.get("rate", "")
    source_type = metadata_info.get("source_type", "telegram")

    rating_str = f"⭐ {rating}" if rating else ""
    if source_type == "torrent":
        help_note = "🧲 Torrent stream. Playback speed depends on seeders/peers.\n\n"
        stream_note = ""
    else:
        help_note = (
            "⚠️ If streaming is slow or not loading, turn on Cloudflare WARP and try again.\n"
            "Facing any issues? Type it here itself.\n\n"
        )
        seek_warning = MkvSeekWarningText if metadata_info.get("mkv_seek_risk") else ""
        stream_note = f"{seek_warning}▶️ <b>Direct Stream Link:</b>\n<code>{direct_stream}</code>"

    if media_type == "tv":
        season = int(metadata_info.get("season_number", 0) or 0)
        episode = int(metadata_info.get("episode_number", 0) or 0)
        ep_title = metadata_info.get("episode_title", "")
        if metadata_info.get("season_pack"):
            episode_count = int(metadata_info.get("season_pack_episode_count", 0) or 0)
            return (
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

        return (
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

    return (
        f"🎬 <b>{movie_title}</b>"
        f"{f' ({year})' if year else ''}\n"
        f"{rating_str}"
        f"{f' | {quality}' if quality else ''}"
        f" | {size}\n\n"
        f"{help_note}"
        f"{stream_note}"
    )
