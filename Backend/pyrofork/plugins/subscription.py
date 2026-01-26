from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from Backend import db
from Backend.config import Telegram
from Backend.logger import LOGGER

# Plan configurations
PLANS = {
    "trial": {"days": 1, "daily_limit_gb": 10, "price": 0},
    "basic": {"days": 30, "daily_limit_gb": 10, "price": 49},
    "vip": {"days": 365, "daily_limit_gb": None, "price": 0},  # VIP for friends - unlimited, 1 year
}

# Maximum paying subscribers (excludes VIP and trial)
MAX_SUBSCRIBERS = 25


async def get_subscriber_count():
    """Get count of active paid subscribers (excludes trial and VIP)"""
    count = await db.dbs["tracking"]["tokens"].count_documents({
        "active": True,
        "plan": {"$nin": ["trial", "vip"]}
    })
    return count


@Client.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    """Welcome message with subscription options"""
    user = message.from_user
    
    # Check if user has active subscription
    existing_token = await db.get_token_by_user(user.id)
    
    if existing_token:
        await message.reply(
            f"👋 **Welcome back, {user.first_name}!**\n\n"
            f"✅ You have an active **{existing_token['plan'].upper()}** subscription\n"
            f"📅 Expires: `{existing_token['expires_at'][:10]}`\n\n"
            f"Use /mytoken to get your addon install link\n"
            f"Use /status to check your usage",
            quote=True
        )
    else:
        await message.reply(
            f"👋 **Welcome to Stremio Addon, {user.first_name}!**\n\n"
            f"🎬 Stream movies and TV shows directly in Stremio!\n\n"
            f"**📋 Plans:**\n"
            f"• **Trial** - FREE for 1 day (10 GB/day)\n"
            f"• **Basic** - ₹49/month (10 GB/day)\n\n"
            f"Use /trial to start your free trial\n"
            f"Use /subscribe to see payment options",
            quote=True
        )


@Client.on_message(filters.command("trial") & filters.private)
async def trial_command(client: Client, message: Message):
    """Get 1-day free trial"""
    user = message.from_user
    
    # Check if user already had a trial or subscription
    existing = await db.get_token_by_user(user.id)
    if existing:
        await message.reply(
            "❌ You already have an active subscription!\n\n"
            "Use /mytoken to get your addon install link.",
            quote=True
        )
        return
    
    # Check if user ever had a trial (one trial per user)
    all_tokens = await db.dbs["tracking"]["tokens"].find_one({
        "user_id": user.id,
        "plan": "trial"
    })
    
    if all_tokens:
        await message.reply(
            "❌ You've already used your free trial!\n\n"
            "Use /subscribe to get a paid subscription.",
            quote=True
        )
        return
    
    # Create trial token
    plan = PLANS["trial"]
    token = await db.create_token(
        user_id=user.id,
        plan="trial",
        days=plan["days"],
        daily_limit_gb=plan["daily_limit_gb"]
    )
    
    install_url = f"{Telegram.BASE_URL}/stremio/{token}/manifest.json"
    
    await message.reply(
        f"🎉 **Trial Activated!**\n\n"
        f"✅ **1 Day Free Trial** (10 GB/day limit)\n\n"
        f"📲 **Install in Stremio:**\n"
        f"Click the button below to install the addon\n\n"
        f"_Your trial expires in 24 hours_",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 Install Addon", url=f"stremio://{install_url}")],
            [InlineKeyboardButton("📋 Copy Link", callback_data=f"copy_{token}")]
        ]),
        quote=True
    )
    
    LOGGER.info(f"Trial activated for user {user.id}")


@Client.on_message(filters.command("subscribe") & filters.private)
async def subscribe_command(client: Client, message: Message):
    """Show subscription options with payment info"""
    # Check subscriber limit
    current_count = await get_subscriber_count()
    
    if current_count >= MAX_SUBSCRIBERS:
        await message.reply(
            "⛔ **All Slots Filled!**\n\n"
            f"We currently have {MAX_SUBSCRIBERS}/{MAX_SUBSCRIBERS} subscribers.\n\n"
            "Please try again later when a slot opens up.",
            quote=True
        )
        return
    
    remaining = MAX_SUBSCRIBERS - current_count
    
    await message.reply(
        "💳 **Subscribe to Stremio Addon**\n\n"
        "**Basic Plan - ₹49/month**\n"
        "• 10 GB streaming per day\n"
        "• 30 days validity\n"
        "• Unlimited catalog access\n\n"
        f"📊 **Slots Available:** {remaining}/{MAX_SUBSCRIBERS}\n\n"
        "**Payment Options:**\n"
        "1️⃣ UPI: `your-upi@paytm`\n"
        "2️⃣ Contact admin after payment\n\n"
        "_After payment, send screenshot to admin_",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📞 Contact Admin", url=f"tg://user?id={Telegram.OWNER_ID}")],
        ]),
        quote=True
    )


@Client.on_message(filters.command("status") & filters.private)
async def status_command(client: Client, message: Message):
    """Check subscription status and usage"""
    user = message.from_user
    
    token_data = await db.get_token_by_user(user.id)
    
    if not token_data:
        await message.reply(
            "❌ No active subscription found!\n\n"
            "Use /trial for a free trial\n"
            "Use /subscribe to get a subscription",
            quote=True
        )
        return
    
    usage = token_data.get("usage", {})
    daily_limit = token_data.get("daily_limit_gb")
    used_today = usage.get("today", 0)
    total_used = usage.get("total", 0)
    
    if daily_limit:
        remaining = max(0, daily_limit - used_today)
        limit_str = f"{daily_limit} GB/day"
        remaining_str = f"{remaining:.2f} GB"
    else:
        limit_str = "Unlimited"
        remaining_str = "Unlimited"
    
    await message.reply(
        f"📊 **Subscription Status**\n\n"
        f"**Plan:** {token_data['plan'].upper()}\n"
        f"**Expires:** {token_data['expires_at'][:10]}\n\n"
        f"**Today's Usage:**\n"
        f"• Used: {used_today:.2f} GB\n"
        f"• Remaining: {remaining_str}\n"
        f"• Limit: {limit_str}\n\n"
        f"**Total Usage:** {total_used:.2f} GB",
        quote=True
    )


@Client.on_message(filters.command("mytoken") & filters.private)
async def mytoken_command(client: Client, message: Message):
    """Get addon install link"""
    user = message.from_user
    
    token_data = await db.get_token_by_user(user.id)
    
    if not token_data:
        await message.reply(
            "❌ No active subscription!\n\n"
            "Use /trial or /subscribe to get started.",
            quote=True
        )
        return
    
    token = token_data["token"]
    install_url = f"{Telegram.BASE_URL}/stremio/{token}/manifest.json"
    
    await message.reply(
        f"📲 **Your Stremio Addon**\n\n"
        f"**Install Link:**\n"
        f"`{install_url}`\n\n"
        f"Click the button below to install directly:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 Install Addon", url=f"stremio://{install_url}")],
        ]),
        quote=True
    )


# Admin commands
@Client.on_message(filters.command("generate") & filters.private & filters.user(Telegram.OWNER_ID))
async def generate_command(client: Client, message: Message):
    """Admin: Generate token for a user. Usage: /generate <user_id> <days> [plan]"""
    args = message.text.split()[1:]
    
    if len(args) < 2:
        await message.reply(
            "**Usage:** `/generate <user_id> <days> [plan]`\n\n"
            "**Plans:** basic, vip\n\n"
            "**Examples:**\n"
            "`/generate 123456789 30 basic` - 30 days basic\n"
            "`/generate 123456789 365 vip` - 1 year VIP (unlimited)",
            quote=True
        )
        return
    
    try:
        user_id = int(args[0])
        days = int(args[1])
        plan = args[2] if len(args) > 2 else "basic"
        
        plan_config = PLANS.get(plan, PLANS["basic"])
        
        token = await db.create_token(
            user_id=user_id,
            plan=plan,
            days=days,
            daily_limit_gb=plan_config["daily_limit_gb"]
        )
        
        install_url = f"{Telegram.BASE_URL}/stremio/{token}/manifest.json"
        
        limit_str = f"{plan_config['daily_limit_gb']} GB/day" if plan_config['daily_limit_gb'] else "Unlimited"
        
        await message.reply(
            f"✅ **Token Generated**\n\n"
            f"**User ID:** `{user_id}`\n"
            f"**Plan:** {plan.upper()}\n"
            f"**Duration:** {days} days\n"
            f"**Limit:** {limit_str}\n"
            f"**Token:** `{token}`\n\n"
            f"**Install URL:**\n`{install_url}`",
            quote=True
        )
        
    except ValueError:
        await message.reply("❌ Invalid user_id or days. Must be numbers.", quote=True)
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}", quote=True)


@Client.on_message(filters.command("vip") & filters.private & filters.user(Telegram.OWNER_ID))
async def vip_command(client: Client, message: Message):
    """Admin: Give VIP access to a friend. Usage: /vip <user_id>"""
    args = message.text.split()[1:]
    
    if not args:
        await message.reply(
            "**Usage:** `/vip <user_id>`\n\n"
            "Gives 1 year unlimited access (no daily limit)",
            quote=True
        )
        return
    
    try:
        user_id = int(args[0])
        
        token = await db.create_token(
            user_id=user_id,
            plan="vip",
            days=365,
            daily_limit_gb=None  # Unlimited
        )
        
        install_url = f"{Telegram.BASE_URL}/stremio/{token}/manifest.json"
        
        await message.reply(
            f"🌟 **VIP Access Granted!**\n\n"
            f"**User ID:** `{user_id}`\n"
            f"**Duration:** 1 Year\n"
            f"**Limit:** Unlimited\n\n"
            f"**Install URL:**\n`{install_url}`",
            quote=True
        )
        
        LOGGER.info(f"VIP access granted to user {user_id}")
        
    except ValueError:
        await message.reply("❌ Invalid user_id. Must be a number.", quote=True)
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}", quote=True)


@Client.on_message(filters.command("revoke") & filters.private & filters.user(Telegram.OWNER_ID))
async def revoke_command(client: Client, message: Message):
    """Admin: Revoke a token. Usage: /revoke <token>"""
    args = message.text.split()[1:]
    
    if not args:
        await message.reply("**Usage:** `/revoke <token>`", quote=True)
        return
    
    token = args[0]
    success = await db.revoke_token(token)
    
    if success:
        await message.reply(f"✅ Token revoked: `{token[:16]}...`", quote=True)
    else:
        await message.reply("❌ Token not found or already revoked", quote=True)


@Client.on_message(filters.command("users") & filters.private & filters.user(Telegram.OWNER_ID))
async def users_command(client: Client, message: Message):
    """Admin: List all subscribers"""
    subscribers = await db.list_subscribers()
    stats = await db.get_subscription_stats()
    paid_count = await get_subscriber_count()
    
    if not subscribers:
        await message.reply("📭 No active subscribers", quote=True)
        return
    
    text = f"👥 **Subscribers: {stats['active_tokens']}** (Paid: {paid_count}/{MAX_SUBSCRIBERS})\n\n"
    
    for sub in subscribers[:20]:  # Limit to 20
        user_id = sub.get("user_id")
        plan = sub.get("plan", "unknown").upper()
        expires = sub.get("expires_at", "")[:10]
        usage = sub.get("usage", {}).get("today", 0)
        
        icon = "🌟" if plan == "VIP" else "👤"
        text += f"{icon} `{user_id}` | {plan} | {expires} | {usage:.1f}GB\n"
    
    if len(subscribers) > 20:
        text += f"\n_...and {len(subscribers) - 20} more_"
    
    text += f"\n\n**By Plan:**\n"
    for plan, count in stats.get("by_plan", {}).items():
        text += f"• {plan}: {count}\n"
    
    await message.reply(text, quote=True)
