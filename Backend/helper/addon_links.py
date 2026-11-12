from Backend.helper.settings_manager import SettingsManager


#----- Addon + Nuvio collection links for a token (Telegram HTML) —
#----- upstream addon_links_text (c43ba47 final shape, no file attachments).
def addon_links_text(token: str) -> str:
    base = f"{SettingsManager.current().base_url}/stremio/{token}"
    return (
        "🧩 <b>Addon Manifest URL (Stremio &amp; Nuvio):</b>\n"
        f"<code>{base}/manifest.json</code>\n\n"
        "📱 <b>Nuvio Collection URLs (Optional):</b>\n"
        f'• <a href="{base}/nuvio-collection.json">Master Collection (All-in-One)</a>\n'
        f'• <a href="{base}/nuvio-discover-collection.json">Discover</a>\n'
        f'• <a href="{base}/nuvio-ott-collection.json">Streaming Platforms</a>\n'
        f'• <a href="{base}/nuvio-franchises-collection.json">Franchises &amp; Universes</a>\n'
        f'• <a href="{base}/nuvio-spotlight-collection.json">Regional &amp; Spotlight</a>\n\n'
        "• Add the <b>Addon Manifest URL</b> into Stremio or Nuvio under Addons.\n"
        "• Import any <b>Nuvio Collection URL</b> in Nuvio (Settings → Collections → Import) for custom tabs and logo buttons on the home screen."
    )
