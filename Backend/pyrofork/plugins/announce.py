from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from Backend.config import Telegram
from Backend.logger import LOGGER


@Client.on_message(filters.command("announce") & filters.user(Telegram.OWNER_ID))
async def announce_addon(client: Client, message: Message):
    """Send addon install message to all AUTH_CHANNEL channels"""
    
    # Use cloudflare tunnel URL
    stremio_url = "stremio://herb-planning-photography-though.trycloudflare.com/stremio/manifest.json"
    addon_url = "https://herb-planning-photography-though.trycloudflare.com/stremio/manifest.json"
    install_url = "https://herb-planning-photography-though.trycloudflare.com/stremio/install"
    
    text = (
        "🎬 <b>Telegram Stremio Addon</b>\n\n"
        "<b>📥 Step 1:</b> Download & Install Stremio\n"
        "👉 <a href='https://www.stremio.com/downloads'>stremio.com/downloads</a>\n\n"
        "<b>👤 Step 2:</b> Open Stremio → Sign Up / Log In\n\n"
        "<b>⬇️ Step 3:</b> Click the button below to add the addon:"
    )
    
    # Use https:// URLs for buttons (Telegram doesn't allow custom protocols like stremio://)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🖥️ Install (Windows/Linux)", url=install_url)],
        [InlineKeyboardButton("📱 Install (Android)", url=install_url)]
    ])
    
    success_count = 0
    fail_count = 0
    
    for channel_id in Telegram.AUTH_CHANNEL:
        try:
            chat_id = int(channel_id) if not channel_id.startswith("-100") else int(channel_id)
            if not str(chat_id).startswith("-100"):
                chat_id = int(f"-100{channel_id}")
            
            sent_msg = await client.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=enums.ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
            
            # Try to pin the message
            try:
                await sent_msg.pin(disable_notification=True)
                LOGGER.info(f"Pinned addon message in channel {chat_id}")
            except Exception as e:
                LOGGER.warning(f"Could not pin message in {chat_id}: {e}")
            
            success_count += 1
            LOGGER.info(f"Sent addon message to channel {chat_id}")
            
        except Exception as e:
            fail_count += 1
            LOGGER.error(f"Failed to send to channel {channel_id}: {e}")
    
    await message.reply_text(
        f"✅ Sent addon message to {success_count} channel(s)\n"
        f"❌ Failed: {fail_count} channel(s)"
    )
