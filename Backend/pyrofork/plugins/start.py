from pyrogram import filters, Client, enums
from Backend.helper.custom_filter import CustomFilters
from pyrogram.types import Message
from Backend.config import Telegram

@Client.on_message(filters.command('start') & filters.private & CustomFilters.owner, group=10)
async def send_start_message(client: Client, message: Message):
    try:
        base_url = Telegram.BASE_URL.rstrip('/')
        install_url = f"{base_url}/stremio/install"

        await message.reply_text(
            '<b>🎬 Telegram Stremio Addon</b>\n\n'
            'Click the link below to install the addon in Stremio:\n\n'
            f'<b>Install URL:</b>\n<code>{install_url}</code>\n\n'
            '👆 Open this link in any browser — it will automatically open Stremio and install the addon on Android, Windows, and Linux.',
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Error: {e}")
        print(f"Error in /start handler: {e}")