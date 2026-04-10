from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from Backend import db
from Backend.config import Telegram
from Backend.helper.custom_filter import CustomFilters


@Client.on_message(filters.command('ops') & filters.private & CustomFilters.owner, group=10)
async def ops_summary(client: Client, message: Message):
    try:
        data = await db.get_usage_dashboard(limit=5)
        summary = data.get('summary', {})
        tokens = data.get('tokens', [])
        recent = data.get('recent', [])
        recent_admin = data.get('recent_admin', [])

        top_token = tokens[0] if tokens else None
        top_line = 'No token data yet.'
        if top_token:
            top_line = (
                f"• {top_token.get('name') or 'Unnamed'} - "
                f"{(top_token.get('daily_bytes') or 0) / (1024 ** 3):.2f} GB today, "
                f"{(top_token.get('monthly_bytes') or 0) / (1024 ** 3):.2f} GB month"
            )

        text = (
            '<b>Operations Summary</b>\n\n'
            f"• Total events: <b>{summary.get('total_events', 0)}</b>\n"
            f"• Stream events: <b>{summary.get('stream_events', 0)}</b>\n"
            f"• Admin actions: <b>{summary.get('admin_events', 0)}</b>\n"
            f"• Failures: <b>{summary.get('failures', 0)}</b>\n"
            f"• Tokens tracked: <b>{summary.get('tracked_tokens', 0)}</b>\n\n"
            f"<b>Top token:</b>\n{top_line}\n\n"
            f"<b>Recent logs:</b> {len(recent)}\n"
            f"<b>Recent admin actions:</b> {len(recent_admin)}\n\n"
            f"<b>Dashboard:</b> {Telegram.BASE_URL}/admin/ops"
        )

        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton('Open Ops Dashboard', url=f'{Telegram.BASE_URL}/admin/ops')],
            [InlineKeyboardButton('Open Public Status', url=f'{Telegram.BASE_URL}/status')],
        ])

        await message.reply_text(
            text,
            quote=True,
            parse_mode=enums.ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=buttons,
        )
    except Exception as e:
        await message.reply_text(f'⚠️ Error: {e}')
