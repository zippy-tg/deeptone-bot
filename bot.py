"""
TikTok Creator Payment Tracker Discord Bot
Main bot file with all commands and event handlers.
"""

import os
import io
import csv
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

from database import Database, VideoRecord, PaymentStatus, CreatorProfile
from utils import (
    TikTokURLParser,
    TikTokScraper,
    calculate_payment,
    format_views,
    format_date,
    format_date_short,
    format_video_id_display,
    format_hours,
    parse_views_input,
    parse_date_input,
    get_status_emoji,
    get_rank_display,
    get_rank_color,
    get_rank_emoji,
    CreatorRank,
    determine_rank,
    get_next_rank,
    views_to_next_rank,
    RANK_THRESHOLDS,
    RANK_CAPS,
    RANK_PAYOUT_TIERS,
    RANK_ORDER,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger("payment_bot")

# Bot configuration
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
COMMAND_PREFIX = os.getenv("COMMAND_PREFIX", "!")

# =============================================================================
# ALLOWED USERS - Owner + authorized creators
# =============================================================================
OWNER_ID = 1354609986902425754
ALLOWED_USERS = {OWNER_ID, 1466569957730025482}
# =============================================================================

# =============================================================================
# RANK ROLE IDS - Discord role IDs for each creator rank
# =============================================================================
RANK_ROLES = {
    CreatorRank.SUB5: 1469123133914087585,
    CreatorRank.LTN: 1469123258140987465,
    CreatorRank.MTN: 1469123321973969000,
    CreatorRank.HTN: 1469123432464519230,
    CreatorRank.CHADLITE: 1470452632970592472,
    CreatorRank.CHAD: 1469123487686856836,
}
# =============================================================================

# Embed colors
COLOR_SUCCESS = 0x2ECC71  # Green
COLOR_ERROR = 0xE74C3C    # Red
COLOR_INFO = 0x3498DB     # Blue
COLOR_WARNING = 0xF1C40F  # Yellow
COLOR_PENDING = 0x9B59B6  # Purple

# Emojis
EMOJI_SUCCESS = "✅"
EMOJI_ERROR = "❌"
EMOJI_CANCEL = "🚫"
EMOJI_CONFIRM = "✔️"
EMOJI_MONEY = "💰"
EMOJI_VIDEO = "🎬"
EMOJI_SEARCH = "🔍"
EMOJI_PENDING = "⏳"
EMOJI_PAID = "💸"

RESPONSE_TIMEOUT = 60

# Thread pool for blocking scraping operations
executor = ThreadPoolExecutor(max_workers=3)


class PaymentBot(commands.Bot):
    """Custom bot class with database integration."""

    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.reactions = True
        intents.members = True  # Needed for role assignment

        super().__init__(
            command_prefix=COMMAND_PREFIX,
            intents=intents,
            help_command=None
        )
        self.db = Database()

    async def setup_hook(self):
        logger.info("Bot is setting up...")
        self.check_eligibility.start()

    async def on_ready(self):
        logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        logger.info(f"Connected to {len(self.guilds)} guild(s)")
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name=f"{COMMAND_PREFIX}help for commands"
            )
        )

    @tasks.loop(minutes=30)
    async def check_eligibility(self):
        """Periodically update pending videos to eligible status."""
        count = self.db.update_pending_to_eligible()
        if count > 0:
            logger.info(f"Auto-updated {count} videos to eligible status")

    @check_eligibility.before_loop
    async def before_check_eligibility(self):
        await self.wait_until_ready()


bot = PaymentBot()


# ============================================================================
# Owner-Only Access Control
# ============================================================================

@bot.event
async def on_message(message):
    """Process messages - only allow owner to use commands."""
    # Ignore bot's own messages
    if message.author.bot:
        return

    # Check if message is a command (starts with prefix)
    if message.content.startswith(COMMAND_PREFIX):
        # Only allow owner to use commands
        if message.author.id not in ALLOWED_USERS:
            await message.channel.send(f"{EMOJI_ERROR} This bot is private and can only be used by the owner.")
            return

    # Process commands only for owner
    await bot.process_commands(message)


# ============================================================================
# Helper Functions
# ============================================================================

def create_embed(title: str, description: str = "", color: int = COLOR_INFO,
                 fields: list = None, footer: str = None) -> discord.Embed:
    """Create a standardized embed."""
    embed = discord.Embed(title=title, description=description, color=color)
    if fields:
        for name, value, inline in fields:
            embed.add_field(name=name, value=value, inline=inline)
    if footer:
        embed.set_footer(text=footer)
    embed.timestamp = datetime.now()
    return embed


async def wait_for_message(ctx: commands.Context, prompt: str,
                           timeout: int = RESPONSE_TIMEOUT) -> Optional[str]:
    """Wait for a message response from the user."""
    await ctx.send(prompt)

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel

    try:
        msg = await bot.wait_for("message", timeout=timeout, check=check)
        if msg.content.lower() in ["cancel", "stop", "exit"]:
            return None
        return msg.content
    except asyncio.TimeoutError:
        await ctx.send(embed=create_embed(
            "⏰ Timeout",
            "You took too long to respond. Operation cancelled.",
            COLOR_WARNING
        ))
        return None


async def confirm_action(ctx: commands.Context, message: str, timeout: int = 30) -> bool:
    """Ask for confirmation using reactions."""
    embed = create_embed("⚠️ Confirm Action", message, COLOR_WARNING)
    embed.set_footer(text=f"React with {EMOJI_CONFIRM} to confirm or {EMOJI_CANCEL} to cancel")

    confirm_msg = await ctx.send(embed=embed)
    await confirm_msg.add_reaction(EMOJI_CONFIRM)
    await confirm_msg.add_reaction(EMOJI_CANCEL)

    def check(reaction, user):
        return (user == ctx.author and
                str(reaction.emoji) in [EMOJI_CONFIRM, EMOJI_CANCEL] and
                reaction.message.id == confirm_msg.id)

    try:
        reaction, _ = await bot.wait_for("reaction_add", timeout=timeout, check=check)
        return str(reaction.emoji) == EMOJI_CONFIRM
    except asyncio.TimeoutError:
        await ctx.send("⏰ Confirmation timed out. Operation cancelled.")
        return False


async def update_creator_role(guild: discord.Guild, creator_name: str, channel: discord.TextChannel = None):
    """Check creator rank and update Discord role if needed. Returns (old_rank, new_rank) if changed."""
    profile = bot.db.get_or_create_creator(creator_name)
    new_rank = profile.current_rank

    if not profile.discord_user_id:
        return None

    member = guild.get_member(profile.discord_user_id)
    if not member:
        try:
            member = await guild.fetch_member(profile.discord_user_id)
        except discord.NotFound:
            return None

    # Get all rank role IDs that exist
    all_rank_role_ids = {rid for rid in RANK_ROLES.values() if rid}

    # Find which rank roles the member currently has
    current_rank_roles = [r for r in member.roles if r.id in all_rank_role_ids]

    # Get the target role
    target_role_id = RANK_ROLES.get(new_rank)
    if not target_role_id:
        return None

    target_role = guild.get_role(target_role_id)
    if not target_role:
        return None

    # Check if they already have the correct role
    if target_role in current_rank_roles and len(current_rank_roles) == 1:
        return None

    # Determine old rank for notification
    old_rank = None
    if current_rank_roles:
        for rank, role_id in RANK_ROLES.items():
            if any(r.id == role_id for r in current_rank_roles):
                old_rank = rank
                break

    # Remove all other rank roles
    roles_to_remove = [r for r in current_rank_roles if r.id != target_role_id]
    if roles_to_remove:
        await member.remove_roles(*roles_to_remove, reason="Rank update")

    # Add new rank role if not already present
    if target_role not in member.roles:
        await member.add_roles(target_role, reason=f"Rank up to {new_rank.value}")

    # Send rank up notification
    if old_rank and old_rank != new_rank and channel:
        old_idx = RANK_ORDER.index(old_rank)
        new_idx = RANK_ORDER.index(new_rank)
        if new_idx > old_idx:
            embed = create_embed(
                "🎉 RANK UP!",
                f"**{creator_name}** has been promoted!\n\n"
                f"{get_rank_display(old_rank)} → {get_rank_display(new_rank)}\n\n"
                f"**Lifetime Views:** {format_views(profile.lifetime_views)}\n"
                f"**New Per-Video Cap:** ${RANK_CAPS[new_rank]}",
                get_rank_color(new_rank)
            )
            await channel.send(embed=embed)

    return (old_rank, new_rank) if old_rank != new_rank else None


def create_payment_breakdown_embed(video: VideoRecord, title: str, color: int, creator_rank: CreatorRank = None) -> discord.Embed:
    """Create an embed showing payment breakdown for a video."""
    # Get creator rank if not provided
    if creator_rank is None:
        profile = bot.db.get_or_create_creator(video.creator_name)
        creator_rank = profile.current_rank

    payment = calculate_payment(video.view_count, creator_rank)

    embed = create_embed(title, "", color)
    embed.add_field(name="Creator", value=video.creator_name, inline=True)
    embed.add_field(name="Rank", value=get_rank_display(creator_rank), inline=True)
    embed.add_field(name="Views", value=format_views(video.view_count), inline=True)
    embed.add_field(name="Status", value=f"{get_status_emoji(video.payment_status.value)} {video.payment_status.value.title()}", inline=True)

    if video.date_posted:
        embed.add_field(name="Posted", value=format_date_short(video.date_posted), inline=True)
    if video.date_eligible:
        if video.is_eligible():
            embed.add_field(name="Eligible", value=f"{format_date_short(video.date_eligible)} ✅", inline=True)
        else:
            hours = video.hours_until_eligible()
            embed.add_field(name="Eligible In", value=format_hours(hours), inline=True)

    # Payment breakdown
    if payment.eligible:
        breakdown = f"**Base:** ${payment.base_payment:.0f} (20k+ qualified)\n"
        if payment.bonuses:
            breakdown += "**Performance Boosts:**\n"
            for threshold, amount in payment.bonuses:
                breakdown += f"  └ {format_views(threshold)} views: +${amount}\n"
        breakdown += f"\n**Total:** ${payment.total_payment:.0f}"
        breakdown += f"\n**Per-Video Cap:** ${RANK_CAPS[creator_rank]}"
        embed.add_field(name="💰 Payment Breakdown", value=breakdown, inline=False)
    else:
        embed.add_field(name="💰 Payment", value="Not eligible (< 20k views)", inline=False)

    embed.add_field(name="Video ID", value=f"`{format_video_id_display(video.video_id)}`", inline=False)
    embed.add_field(name="URL", value=video.url, inline=False)

    return embed


# ============================================================================
# Commands
# ============================================================================

@bot.command(name="help")
async def help_command(ctx: commands.Context):
    """Display all available commands."""
    embed = create_embed(
        "📚 Payment Tracker Commands",
        "Track TikTok creator payments with automatic view counting.",
        COLOR_INFO
    )

    submission = [
        (f"`{COMMAND_PREFIX}submit [URL]`", "Submit video (auto-fetches views & date)"),
        (f"`{COMMAND_PREFIX}updateviews [id] [views]`", "Update view count for a video"),
    ]

    status_cmds = [
        (f"`{COMMAND_PREFIX}unpaid`", "All unpaid videos with IDs"),
        (f"`{COMMAND_PREFIX}owed`", "Quick list of unpaid video IDs"),
        (f"`{COMMAND_PREFIX}pending`", "Videos waiting for 48hr eligibility"),
        (f"`{COMMAND_PREFIX}eligible`", "Videos ready for payment"),
    ]

    payment_cmds = [
        (f"`{COMMAND_PREFIX}markpaid [id]`", "Mark video as paid"),
        (f"`{COMMAND_PREFIX}reject [id] [reason]`", "Reject a payment"),
    ]

    rank_cmds = [
        (f"`{COMMAND_PREFIX}rank [creator]`", "Show creator rank & progress"),
        (f"`{COMMAND_PREFIX}ranks`", "All creators ranked by views"),
        (f"`{COMMAND_PREFIX}ladder`", "Full earnings ladder tiers"),
        (f"`{COMMAND_PREFIX}setcreator @user name`", "Link Discord user to creator"),
        (f"`{COMMAND_PREFIX}giverole @user rank`", "Manually assign rank role"),
    ]

    reports = [
        (f"`{COMMAND_PREFIX}stats`", "Overall statistics"),
        (f"`{COMMAND_PREFIX}creator [name]`", "Videos for specific creator"),
        (f"`{COMMAND_PREFIX}recent`", "Last 10 submissions"),
        (f"`{COMMAND_PREFIX}weekly`", "Weekly payout report (CSV)"),
        (f"`{COMMAND_PREFIX}export`", "Export all records (CSV)"),
        (f"`{COMMAND_PREFIX}viewhistory [id]`", "View count history"),
    ]

    management = [
        (f"`{COMMAND_PREFIX}lookup [id]`", "Look up a video by ID"),
        (f"`{COMMAND_PREFIX}delete [id]`", "Delete a video record"),
    ]

    for name, cmds in [("📥 Submission", submission), ("📊 Status", status_cmds),
                       ("💳 Payments", payment_cmds), ("🏆 Ranks", rank_cmds),
                       ("📈 Reports", reports), ("🔧 Management", management)]:
        value = "\n".join([f"{cmd} - {desc}" for cmd, desc in cmds])
        embed.add_field(name=name, value=value, inline=False)

    embed.set_footer(text="Type 'cancel' during any operation to abort")
    await ctx.send(embed=embed)


@bot.command(name="submit")
async def submit_video(ctx: commands.Context, url: str = None):
    """Submit a TikTok video for payment tracking with auto-scraping."""
    if not url:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing URL",
            f"Usage: `{COMMAND_PREFIX}submit [TikTok URL]`",
            COLOR_ERROR
        ))
        return

    # Parse URL
    await ctx.message.add_reaction(EMOJI_SEARCH)

    video_id, parsed_url, detected_username = await TikTokURLParser.parse_url(url)

    if not video_id:
        await ctx.message.remove_reaction(EMOJI_SEARCH, bot.user)
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Invalid URL",
            "Could not parse the TikTok URL.",
            COLOR_ERROR
        ))
        return

    # Check for duplicate
    existing = bot.db.check_duplicate(video_id)
    if existing:
        await ctx.message.remove_reaction(EMOJI_SEARCH, bot.user)
        await ctx.message.add_reaction(EMOJI_ERROR)
        embed = create_payment_breakdown_embed(
            existing,
            f"{EMOJI_ERROR} Duplicate - Already Tracked",
            COLOR_ERROR
        )
        await ctx.send(embed=embed)
        return

    # Scrape video data
    await ctx.send(f"{EMOJI_SEARCH} **Fetching video data...**")

    loop = asyncio.get_event_loop()
    scraped_data = await loop.run_in_executor(executor, TikTokScraper.scrape_video, parsed_url)

    await ctx.message.remove_reaction(EMOJI_SEARCH, bot.user)

    # Use scraped data or fall back to manual input
    views = scraped_data.views
    date_posted = scraped_data.date_posted
    creator_name = scraped_data.username or detected_username

    # Show what we found (if anything)
    if views and date_posted:
        # Full scrape successful
        desc_parts = [
            f"**Creator:** `{creator_name}`",
            f"**Views:** {format_views(views)} ({views:,})",
            f"**Posted:** {format_date(date_posted)}"
        ]
        embed = create_embed(
            f"{EMOJI_SUCCESS} Video Data Found!",
            "\n".join(desc_parts),
            COLOR_SUCCESS
        )
        embed.set_footer(text="Type 'cancel' to abort")
        await ctx.send(embed=embed)

        # Confirm or correct
        response = await wait_for_message(
            ctx,
            "Is this correct? Reply `yes` to confirm, or type a correction:"
        )
        if not response:
            await ctx.message.add_reaction(EMOJI_CANCEL)
            return

        if response.lower() not in ["yes", "y", "correct", "ok"]:
            parsed_views = parse_views_input(response)
            if parsed_views:
                views = parsed_views
            else:
                creator_name = response
    else:
        # Partial or no scrape - need manual input
        if creator_name:
            await ctx.send(embed=create_embed(
                f"{EMOJI_VIDEO} Creator Detected",
                f"**Creator:** `{creator_name}`\n\n"
                "⚠️ Could not auto-fetch views/date from TikTok.\n"
                "Please enter them manually below.",
                COLOR_WARNING
            ))

            # Confirm creator name
            response = await wait_for_message(
                ctx,
                f"👤 Is **{creator_name}** correct? Reply `yes` or type the correct name:"
            )
            if not response:
                await ctx.message.add_reaction(EMOJI_CANCEL)
                return
            if response.lower() not in ["yes", "y", "ok"]:
                creator_name = response
        else:
            await ctx.send(embed=create_embed(
                "⚠️ Manual Entry Required",
                "Could not fetch data from TikTok automatically.",
                COLOR_WARNING
            ))
            creator_name = await wait_for_message(ctx, "👤 Enter **creator name**:")
            if not creator_name:
                await ctx.message.add_reaction(EMOJI_CANCEL)
                return

        # Get views manually
        views_str = await wait_for_message(
            ctx,
            "👁️ Enter **view count** (e.g., `45000` or `45k` or `1.2m`):"
        )
        if not views_str:
            await ctx.message.add_reaction(EMOJI_CANCEL)
            return

        views = parse_views_input(views_str)
        if not views:
            await ctx.send(embed=create_embed(
                f"{EMOJI_ERROR} Invalid View Count",
                f"Could not parse: `{views_str}`",
                COLOR_ERROR
            ))
            await ctx.message.add_reaction(EMOJI_ERROR)
            return

        # Get date manually
        date_str = await wait_for_message(
            ctx,
            "📅 When was this posted?\n"
            "Examples: `today`, `yesterday`, `2026-01-20`, `3 days ago`"
        )
        if not date_str:
            await ctx.message.add_reaction(EMOJI_CANCEL)
            return

        date_posted = parse_date_input(date_str)
        if not date_posted:
            await ctx.send(embed=create_embed(
                f"{EMOJI_ERROR} Invalid Date",
                f"Could not parse: `{date_str}`",
                COLOR_ERROR
            ))
            await ctx.message.add_reaction(EMOJI_ERROR)
            return

    # Get creator rank for payment calculation
    profile = bot.db.get_or_create_creator(creator_name)
    creator_rank = profile.current_rank

    # Calculate payment based on rank
    payment = calculate_payment(views, creator_rank)
    date_eligible = date_posted + timedelta(hours=48)
    hours_until = max(0, (date_eligible - datetime.now()).total_seconds() / 3600)

    # Show preview
    preview_embed = create_embed("📋 Confirm Submission", "", COLOR_WARNING)
    preview_embed.add_field(name="Creator", value=creator_name, inline=True)
    preview_embed.add_field(name="Rank", value=get_rank_display(creator_rank), inline=True)
    preview_embed.add_field(name="Views", value=format_views(views), inline=True)
    preview_embed.add_field(name="Posted", value=format_date_short(date_posted), inline=True)

    if hours_until > 0:
        preview_embed.add_field(name="Eligible In", value=format_hours(hours_until), inline=True)
    else:
        preview_embed.add_field(name="Eligible", value="Now ✅", inline=True)

    if payment.eligible:
        preview_embed.add_field(name="Payment", value=f"${payment.total_payment:.0f} (cap: ${RANK_CAPS[creator_rank]})", inline=True)
    else:
        preview_embed.add_field(name="Payment", value="Not eligible (< 20k)", inline=True)

    await ctx.send(embed=preview_embed)

    if not await confirm_action(ctx, "Save this video?"):
        await ctx.message.add_reaction(EMOJI_CANCEL)
        return

    # Save to database
    try:
        video = bot.db.add_video(
            video_id=video_id,
            url=parsed_url,
            creator_name=creator_name,
            view_count=views,
            date_posted=date_posted,
            base_payment=payment.base_payment,
            bonus_amount=payment.bonus_amount,
            total_payment=payment.total_payment,
            needs_custom_bonus=payment.needs_custom_bonus
        )

        await ctx.message.add_reaction(EMOJI_SUCCESS)

        result_embed = create_payment_breakdown_embed(
            video,
            f"{EMOJI_SUCCESS} Video Logged Successfully",
            COLOR_SUCCESS,
            creator_rank
        )
        await ctx.send(embed=result_embed)
        logger.info(f"Added video {video_id} - {creator_name} - {views} views - Rank: {creator_rank.value}")

        # Check for rank change and update role
        if ctx.guild:
            await update_creator_role(ctx.guild, creator_name, ctx.channel)

    except Exception as e:
        logger.error(f"Failed to save video: {e}")
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Database Error",
            str(e),
            COLOR_ERROR
        ))


@bot.command(name="updateviews")
async def update_views(ctx: commands.Context, video_id: str = None, new_views: str = None):
    """Update view count for a video and recalculate payment."""
    if not video_id or not new_views:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Parameters",
            f"Usage: `{COMMAND_PREFIX}updateviews [video_id] [new_views]`\n"
            f"Example: `{COMMAND_PREFIX}updateviews 7123456789 85000`",
            COLOR_ERROR
        ))
        return

    existing = bot.db.get_video_by_id(video_id)
    if not existing:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Video Not Found",
            f"No record found with ID: `{video_id}`",
            COLOR_ERROR
        ))
        return

    views = parse_views_input(new_views)
    if not views:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Invalid View Count",
            f"Could not parse: `{new_views}`",
            COLOR_ERROR
        ))
        return

    # Get creator rank
    profile = bot.db.get_or_create_creator(existing.creator_name)
    creator_rank = profile.current_rank

    old_views = existing.view_count
    old_payment = existing.total_payment
    payment = calculate_payment(views, creator_rank)

    # Check for suspicious changes
    warnings = []
    if views < old_views:
        warnings.append(f"⚠️ Views decreased ({format_views(old_views)} → {format_views(views)})")
    if views > old_views * 10 and (views - old_views) > 100000:
        warnings.append(f"⚠️ Suspicious growth (+{format_views(views - old_views)} in short time)")

    # Show comparison
    embed = create_embed("📊 View Update Preview", "", COLOR_WARNING)
    embed.add_field(name="Creator", value=existing.creator_name, inline=True)
    embed.add_field(name="Rank", value=get_rank_display(creator_rank), inline=True)
    embed.add_field(name="Old Views", value=format_views(old_views), inline=True)
    embed.add_field(name="New Views", value=format_views(views), inline=True)
    embed.add_field(name="Old Payment", value=f"${old_payment:.0f}", inline=True)
    embed.add_field(name="New Payment", value=f"${payment.total_payment:.0f}", inline=True)
    embed.add_field(name="Difference", value=f"+{format_views(views - old_views)}, +${payment.total_payment - old_payment:.0f}", inline=True)

    if warnings:
        embed.add_field(name="⚠️ Warnings", value="\n".join(warnings), inline=False)

    await ctx.send(embed=embed)

    if not await confirm_action(ctx, "Apply this update?"):
        return

    updated = bot.db.update_views(
        video_id=video_id,
        new_views=views,
        base_payment=payment.base_payment,
        bonus_amount=payment.bonus_amount,
        total_payment=payment.total_payment,
        needs_custom_bonus=payment.needs_custom_bonus
    )

    if updated:
        await ctx.message.add_reaction(EMOJI_SUCCESS)
        await ctx.send(embed=create_embed(
            f"{EMOJI_SUCCESS} Views Updated",
            f"**{existing.creator_name}**: {format_views(old_views)} → {format_views(views)}\n"
            f"**Payment**: ${old_payment:.0f} → ${payment.total_payment:.0f}",
            COLOR_SUCCESS
        ))

        # Check for rank change and update role
        if ctx.guild:
            await update_creator_role(ctx.guild, existing.creator_name, ctx.channel)
    else:
        await ctx.send(embed=create_embed(f"{EMOJI_ERROR} Update Failed", "", COLOR_ERROR))


@bot.command(name="pending")
async def show_pending(ctx: commands.Context):
    """Show videos waiting for 48hr eligibility."""
    videos = bot.db.get_pending_videos()

    if not videos:
        await ctx.send(embed=create_embed(
            f"{EMOJI_PENDING} Pending Videos",
            "No videos waiting for eligibility.",
            COLOR_INFO
        ))
        return

    embed = create_embed(
        f"{EMOJI_PENDING} Pending Eligibility ({len(videos)} videos)",
        "Videos waiting for 48-hour window",
        COLOR_PENDING
    )

    for v in videos[:15]:
        hours = v.hours_until_eligible()
        eligible_status = "✅ Now" if hours <= 0 else format_hours(hours)
        payment = calculate_payment(v.view_count)

        embed.add_field(
            name=v.creator_name,
            value=f"Views: {format_views(v.view_count)}\n"
                  f"Est: ${payment.total_payment:.0f}\n"
                  f"Eligible: {eligible_status}",
            inline=True
        )

    if len(videos) > 15:
        embed.set_footer(text=f"Showing 15 of {len(videos)} videos")

    await ctx.send(embed=embed)


@bot.command(name="eligible")
async def show_eligible(ctx: commands.Context):
    """Show videos eligible for payment."""
    videos = bot.db.get_eligible_videos()

    if not videos:
        await ctx.send(embed=create_embed(
            f"{EMOJI_SUCCESS} Eligible Videos",
            "No videos currently eligible for payment.",
            COLOR_INFO
        ))
        return

    total_owed = sum(v.total_payment for v in videos)

    embed = create_embed(
        f"{EMOJI_SUCCESS} Eligible for Payment ({len(videos)} videos)",
        f"**Total Owed: ${total_owed:,.0f}**",
        COLOR_SUCCESS
    )

    for v in videos[:15]:
        embed.add_field(
            name=v.creator_name,
            value=f"Views: {format_views(v.view_count)}\n"
                  f"Amount: ${v.total_payment:.0f}\n"
                  f"Posted: {format_date_short(v.date_posted)}",
            inline=True
        )

    if len(videos) > 15:
        embed.set_footer(text=f"Showing 15 of {len(videos)} videos")

    await ctx.send(embed=embed)


@bot.command(name="unpaid")
async def show_unpaid(ctx: commands.Context):
    """Show ALL videos not yet paid with their IDs."""
    pending = bot.db.get_pending_videos()
    eligible = bot.db.get_eligible_videos()

    all_unpaid = eligible + pending  # Eligible first, then pending

    if not all_unpaid:
        await ctx.send(embed=create_embed(
            "💰 Unpaid Videos",
            "No unpaid videos! All caught up.",
            COLOR_SUCCESS
        ))
        return

    total_owed = sum(v.total_payment for v in all_unpaid)

    embed = create_embed(
        f"💰 Unpaid Videos ({len(all_unpaid)} total)",
        f"**Total Owed: ${total_owed:,.0f}**\n"
        f"✅ Eligible: {len(eligible)} | ⏳ Pending: {len(pending)}\n\n"
        f"_Use `{COMMAND_PREFIX}markpaid [video_id]` to mark as paid_",
        COLOR_INFO
    )

    # Show eligible videos first (ready to pay)
    if eligible:
        eligible_list = ""
        for v in eligible[:8]:
            eligible_list += f"**{v.creator_name}** - ${v.total_payment:.0f}\n"
            eligible_list += f"└ ID: `{v.video_id}`\n"
        if len(eligible) > 8:
            eligible_list += f"_...and {len(eligible) - 8} more_"
        embed.add_field(name="✅ Ready to Pay", value=eligible_list or "None", inline=False)

    # Show pending videos
    if pending:
        pending_list = ""
        for v in pending[:8]:
            hours = v.hours_until_eligible()
            pending_list += f"**{v.creator_name}** - ${v.total_payment:.0f} (in {format_hours(hours)})\n"
            pending_list += f"└ ID: `{v.video_id}`\n"
        if len(pending) > 8:
            pending_list += f"_...and {len(pending) - 8} more_"
        embed.add_field(name="⏳ Waiting for 48hrs", value=pending_list or "None", inline=False)

    await ctx.send(embed=embed)


@bot.command(name="owed")
async def show_owed(ctx: commands.Context):
    """Quick list of all unpaid video IDs for easy copy-paste."""
    pending = bot.db.get_pending_videos()
    eligible = bot.db.get_eligible_videos()

    all_unpaid = eligible + pending

    if not all_unpaid:
        await ctx.send(embed=create_embed(
            "💰 Nothing Owed",
            "All videos have been paid!",
            COLOR_SUCCESS
        ))
        return

    total_owed = sum(v.total_payment for v in all_unpaid)

    # Create a compact list
    lines = [f"**Total: ${total_owed:,.0f}** ({len(all_unpaid)} videos)\n"]

    for v in all_unpaid:
        status = "✅" if v.payment_status.value == "eligible" else "⏳"
        lines.append(f"{status} `{v.video_id}` | {v.creator_name} | ${v.total_payment:.0f}")

    # Split into chunks if too long
    message = "\n".join(lines)
    if len(message) > 4000:
        message = "\n".join(lines[:30]) + f"\n\n_...and {len(all_unpaid) - 30} more. Use `{COMMAND_PREFIX}export` for full list._"

    embed = create_embed("💰 All Unpaid Videos", message, COLOR_INFO)
    embed.set_footer(text=f"Use {COMMAND_PREFIX}markpaid [video_id] to mark as paid")
    await ctx.send(embed=embed)


@bot.command(name="markpaid")
async def mark_paid(ctx: commands.Context, video_id: str = None):
    """Mark a video as paid."""
    if not video_id:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Video ID",
            f"Usage: `{COMMAND_PREFIX}markpaid [video_id]`",
            COLOR_ERROR
        ))
        return

    existing = bot.db.get_video_by_id(video_id)
    if not existing:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Video Not Found",
            f"No record found with ID: `{video_id}`",
            COLOR_ERROR
        ))
        return

    if existing.payment_status == PaymentStatus.PAID:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Already Paid",
            f"This video was already marked as paid on {format_date(existing.date_paid)}",
            COLOR_ERROR
        ))
        return

    # Show what will be marked
    embed = create_payment_breakdown_embed(existing, "💳 Mark as Paid?", COLOR_WARNING)
    await ctx.send(embed=embed)

    if not await confirm_action(ctx, f"Mark payment of ${existing.total_payment:.0f} to {existing.creator_name} as paid?"):
        return

    updated = bot.db.mark_paid(video_id)
    if updated:
        await ctx.message.add_reaction(EMOJI_PAID)
        await ctx.send(embed=create_embed(
            f"{EMOJI_PAID} Payment Recorded",
            f"**Creator:** {existing.creator_name}\n"
            f"**Amount:** ${existing.total_payment:.0f}\n"
            f"**Paid:** {format_date(datetime.now())}",
            COLOR_SUCCESS
        ))
        logger.info(f"Marked {video_id} as paid - ${existing.total_payment}")
    else:
        await ctx.send(embed=create_embed(f"{EMOJI_ERROR} Failed to update", "", COLOR_ERROR))


@bot.command(name="reject")
async def reject_payment(ctx: commands.Context, video_id: str = None, *, reason: str = None):
    """Reject a video payment."""
    if not video_id:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Parameters",
            f"Usage: `{COMMAND_PREFIX}reject [video_id] [reason]`\n"
            f"Reasons: `botted`, `low effort`, `stolen`, `off-topic`, or custom",
            COLOR_ERROR
        ))
        return

    existing = bot.db.get_video_by_id(video_id)
    if not existing:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Video Not Found",
            f"No record found with ID: `{video_id}`",
            COLOR_ERROR
        ))
        return

    if not reason:
        reason = await wait_for_message(
            ctx,
            "📝 Enter rejection reason (`botted`, `low effort`, `stolen`, `off-topic`, or custom):"
        )
        if not reason:
            return

    embed = create_payment_breakdown_embed(existing, "❌ Reject Payment?", COLOR_WARNING)
    embed.add_field(name="Rejection Reason", value=reason, inline=False)
    await ctx.send(embed=embed)

    if not await confirm_action(ctx, "Reject this payment?"):
        return

    updated = bot.db.reject_payment(video_id, reason)
    if updated:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Payment Rejected",
            f"**Creator:** {existing.creator_name}\n"
            f"**Reason:** {reason}",
            COLOR_ERROR
        ))
        logger.info(f"Rejected {video_id}: {reason}")
    else:
        await ctx.send(embed=create_embed(f"{EMOJI_ERROR} Failed to update", "", COLOR_ERROR))


@bot.command(name="stats")
async def show_stats(ctx: commands.Context):
    """Show overall statistics."""
    stats = bot.db.get_stats()

    embed = create_embed("📊 Payment Statistics", "", COLOR_INFO)

    # Video counts
    counts = (
        f"**Total Videos:** {stats.total_videos}\n"
        f"├─ Pending: {stats.pending_count}\n"
        f"├─ Eligible: {stats.eligible_count}\n"
        f"├─ Paid: {stats.paid_count}\n"
        f"└─ Rejected: {stats.rejected_count}"
    )
    embed.add_field(name="📹 Videos", value=counts, inline=False)

    # Financial
    financial = (
        f"**Owed (unpaid):** ${stats.total_owed:,.0f}\n"
        f"**Paid (all time):** ${stats.total_paid:,.0f}\n"
        f"**Avg per video:** ${stats.average_per_video:,.0f}\n"
        f"**Highest payout:** ${stats.highest_payout:,.0f}"
    )
    embed.add_field(name="💰 Financial", value=financial, inline=False)

    # Top earner
    if stats.top_earner_week:
        name, amount, count = stats.top_earner_week
        embed.add_field(
            name="🏆 Top Earner This Week",
            value=f"**{name}** - ${amount:,.0f} ({count} videos)",
            inline=False
        )

    embed.add_field(name="👥 Creators", value=str(stats.unique_creators), inline=True)

    await ctx.send(embed=embed)


@bot.command(name="creator")
async def show_creator(ctx: commands.Context, *, creator_name: str = None):
    """Show all videos for a specific creator."""
    if not creator_name:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Creator Name",
            f"Usage: `{COMMAND_PREFIX}creator [name]`",
            COLOR_ERROR
        ))
        return

    videos = bot.db.get_creator_videos(creator_name)

    if not videos:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Creator Not Found",
            f"No records for: **{creator_name}**",
            COLOR_ERROR
        ))
        return

    # Get rank info
    profile = bot.db.get_or_create_creator(creator_name)

    total_views = profile.lifetime_views
    total_paid = profile.total_paid
    total_owed = profile.unpaid_amount

    # Progress to next rank
    next_rank = get_next_rank(profile.current_rank)
    progress_text = ""
    if next_rank:
        remaining = views_to_next_rank(profile.current_rank, total_views)
        next_threshold = RANK_THRESHOLDS[next_rank]
        progress_pct = min(100, (total_views / next_threshold) * 100)
        progress_text = f"\n**Next Rank:** {get_rank_display(next_rank)} ({format_views(remaining)} views away - {progress_pct:.0f}%)"
    else:
        progress_text = "\n**Max Rank Achieved!** 👑"

    embed = create_embed(
        f"👤 {videos[0].creator_name}",
        f"**Rank:** {get_rank_display(profile.current_rank)}\n"
        f"**{len(videos)} videos** | **{format_views(total_views)} lifetime views**\n"
        f"**Paid:** ${total_paid:,.0f} | **Owed:** ${total_owed:,.0f}\n"
        f"**Per-Video Cap:** ${RANK_CAPS[profile.current_rank]}"
        f"{progress_text}",
        get_rank_color(profile.current_rank)
    )

    for v in videos[:10]:
        status_emoji = get_status_emoji(v.payment_status.value)
        embed.add_field(
            name=f"{status_emoji} {format_date_short(v.date_posted)}",
            value=f"Views: {format_views(v.view_count)}\n"
                  f"${v.total_payment:.0f}",
            inline=True
        )

    if len(videos) > 10:
        embed.set_footer(text=f"Showing 10 of {len(videos)} videos")

    await ctx.send(embed=embed)


@bot.command(name="recent")
async def show_recent(ctx: commands.Context, count: int = 10):
    """Show recent video submissions."""
    count = min(max(1, count), 20)
    videos = bot.db.get_recent_videos(count)

    if not videos:
        await ctx.send(embed=create_embed(
            "📋 Recent Submissions",
            "No videos logged yet.",
            COLOR_INFO
        ))
        return

    embed = create_embed(f"📋 Last {len(videos)} Submissions", "", COLOR_INFO)

    for v in videos:
        status_emoji = get_status_emoji(v.payment_status.value)
        embed.add_field(
            name=f"{status_emoji} {v.creator_name}",
            value=f"Views: {format_views(v.view_count)}\n"
                  f"${v.total_payment:.0f} | {format_date_short(v.date_submitted)}",
            inline=True
        )

    await ctx.send(embed=embed)


@bot.command(name="viewhistory")
async def view_history(ctx: commands.Context, video_id: str = None):
    """Show view count history for a video."""
    if not video_id:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Video ID",
            f"Usage: `{COMMAND_PREFIX}viewhistory [video_id]`",
            COLOR_ERROR
        ))
        return

    video = bot.db.get_video_by_id(video_id)
    if not video:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Video Not Found",
            f"No record with ID: `{video_id}`",
            COLOR_ERROR
        ))
        return

    embed = create_embed(
        f"📈 View History - {video.creator_name}",
        f"Video ID: `{format_video_id_display(video_id)}`",
        COLOR_INFO
    )

    history_text = ""
    prev_views = 0
    for entry in video.view_count_history:
        diff = f"+{format_views(entry.views - prev_views)}" if prev_views else ""
        history_text += f"**{entry.date}:** {format_views(entry.views)} {diff}\n"
        history_text += f"  └ {entry.note}\n"
        prev_views = entry.views

    embed.add_field(name="History", value=history_text or "No history", inline=False)
    embed.add_field(name="Current Views", value=format_views(video.view_count), inline=True)
    embed.add_field(name="Current Payment", value=f"${video.total_payment:.0f}", inline=True)

    await ctx.send(embed=embed)


@bot.command(name="weekly")
async def weekly_report(ctx: commands.Context):
    """Generate weekly payout report."""
    report = bot.db.get_weekly_report()

    if not report:
        await ctx.send(embed=create_embed(
            "📅 Weekly Report",
            "No payouts this week.",
            COLOR_INFO
        ))
        return

    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Creator", "Videos", "Total_Views", "Base_Pay", "Bonuses", "Total_Owed"])

    total_owed = 0
    for row in report:
        writer.writerow([
            row["creator"],
            row["videos"],
            row["total_views"],
            f"{row['base_pay']:.2f}",
            f"{row['bonuses']:.2f}",
            f"{row['total_owed']:.2f}"
        ])
        total_owed += row["total_owed"]

    output.seek(0)
    csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
    filename = f"weekly_payouts_{datetime.now().strftime('%Y-%m-%d')}.csv"

    # Summary embed
    embed = create_embed(
        "📅 Weekly Payout Report",
        f"**{len(report)} creators** | **Total: ${total_owed:,.0f}**",
        COLOR_INFO
    )

    for row in report[:10]:
        embed.add_field(
            name=row["creator"],
            value=f"{row['videos']} videos\n${row['total_owed']:.0f}",
            inline=True
        )

    await ctx.send(embed=embed, file=discord.File(csv_bytes, filename=filename))


@bot.command(name="export")
async def export_csv(ctx: commands.Context):
    """Export all records to CSV."""
    data = bot.db.export_to_csv_data()

    if not data:
        await ctx.send(embed=create_embed("📁 Export", "No records to export.", COLOR_INFO))
        return

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Date Submitted", "Creator", "Video ID", "URL", "Views",
        "Base Pay", "Bonus", "Total", "Status", "Date Posted", "Date Paid", "Notes"
    ])
    writer.writerows(data)

    output.seek(0)
    csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
    filename = f"all_payments_{datetime.now().strftime('%Y-%m-%d')}.csv"

    await ctx.send(
        embed=create_embed(
            "📁 Export Complete",
            f"Exported **{len(data)}** records.",
            COLOR_SUCCESS
        ),
        file=discord.File(csv_bytes, filename=filename)
    )


@bot.command(name="lookup")
async def lookup_video(ctx: commands.Context, video_id: str = None):
    """Look up a specific video by ID."""
    if not video_id:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Video ID",
            f"Usage: `{COMMAND_PREFIX}lookup [video_id]`",
            COLOR_ERROR
        ))
        return

    video = bot.db.get_video_by_id(video_id)
    if not video:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Video Not Found",
            f"No record with ID: `{video_id}`",
            COLOR_ERROR
        ))
        return

    color = {
        PaymentStatus.PENDING: COLOR_PENDING,
        PaymentStatus.ELIGIBLE: COLOR_SUCCESS,
        PaymentStatus.PAID: COLOR_INFO,
        PaymentStatus.REJECTED: COLOR_ERROR
    }.get(video.payment_status, COLOR_INFO)

    embed = create_payment_breakdown_embed(video, f"{EMOJI_VIDEO} Video Details", color)
    await ctx.send(embed=embed)


@bot.command(name="delete")
async def delete_video(ctx: commands.Context, video_id: str = None):
    """Delete a video record."""
    if not video_id:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Video ID",
            f"Usage: `{COMMAND_PREFIX}delete [video_id]`",
            COLOR_ERROR
        ))
        return

    existing = bot.db.get_video_by_id(video_id)
    if not existing:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Video Not Found",
            f"No record with ID: `{video_id}`",
            COLOR_ERROR
        ))
        return

    embed = create_payment_breakdown_embed(existing, "🗑️ Delete This Record?", COLOR_WARNING)
    await ctx.send(embed=embed)

    if not await confirm_action(ctx, "⚠️ This cannot be undone. Delete this record?"):
        return

    if bot.db.delete_video(video_id):
        await ctx.message.add_reaction(EMOJI_SUCCESS)
        await ctx.send(embed=create_embed(
            f"{EMOJI_SUCCESS} Record Deleted",
            f"Deleted `{format_video_id_display(video_id)}` by {existing.creator_name}",
            COLOR_SUCCESS
        ))
        logger.info(f"Deleted {video_id}")
    else:
        await ctx.send(embed=create_embed(f"{EMOJI_ERROR} Delete Failed", "", COLOR_ERROR))


# ============================================================================
# Rank Commands
# ============================================================================

@bot.command(name="rank")
async def show_rank(ctx: commands.Context, *, creator_name: str = None):
    """Show a creator's rank, lifetime views, and progress to next rank."""
    if not creator_name:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Creator Name",
            f"Usage: `{COMMAND_PREFIX}rank [creator_name]`",
            COLOR_ERROR
        ))
        return

    videos = bot.db.get_creator_videos(creator_name)
    if not videos:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Creator Not Found",
            f"No records for: **{creator_name}**",
            COLOR_ERROR
        ))
        return

    profile = bot.db.get_or_create_creator(creator_name)
    rank = profile.current_rank

    embed = create_embed(
        f"{get_rank_emoji(rank)} {creator_name}'s Rank",
        "",
        get_rank_color(rank)
    )

    embed.add_field(name="Current Rank", value=get_rank_display(rank), inline=True)
    embed.add_field(name="Lifetime Views", value=format_views(profile.lifetime_views), inline=True)
    embed.add_field(name="Videos", value=str(profile.video_count), inline=True)
    embed.add_field(name="Per-Video Cap", value=f"${RANK_CAPS[rank]}", inline=True)
    embed.add_field(name="Total Paid", value=f"${profile.total_paid:,.0f}", inline=True)
    embed.add_field(name="Owed", value=f"${profile.unpaid_amount:,.0f}", inline=True)

    # Show payout tiers for their rank
    tiers = RANK_PAYOUT_TIERS[rank]
    tiers_text = ""
    for threshold, amount in tiers:
        tiers_text += f"${amount} at {format_views(threshold)} views\n"
    embed.add_field(name="💰 Payout Tiers", value=tiers_text, inline=False)

    # Progress to next rank
    next_rank = get_next_rank(rank)
    if next_rank:
        remaining = views_to_next_rank(rank, profile.lifetime_views)
        next_threshold = RANK_THRESHOLDS[next_rank]
        current_threshold = RANK_THRESHOLDS[rank]
        progress = profile.lifetime_views - current_threshold
        total_needed = next_threshold - current_threshold
        progress_pct = min(100, (progress / total_needed) * 100) if total_needed > 0 else 0

        # Progress bar
        filled = int(progress_pct / 10)
        bar = "█" * filled + "░" * (10 - filled)

        embed.add_field(
            name=f"📈 Progress to {get_rank_display(next_rank)}",
            value=f"`[{bar}]` {progress_pct:.0f}%\n"
                  f"{format_views(remaining)} views remaining\n"
                  f"Need: {format_views(next_threshold)} lifetime views",
            inline=False
        )
    else:
        embed.add_field(name="👑 Max Rank!", value="You've reached the highest rank!", inline=False)

    await ctx.send(embed=embed)


@bot.command(name="ranks")
async def show_all_ranks(ctx: commands.Context):
    """Show all creators and their ranks."""
    creators = bot.db.get_all_creators_with_ranks()

    if not creators:
        await ctx.send(embed=create_embed(
            "🏆 Creator Rankings",
            "No creators registered yet.",
            COLOR_INFO
        ))
        return

    embed = create_embed(
        "🏆 Creator Rankings",
        f"**{len(creators)} creators** ranked by lifetime views",
        COLOR_INFO
    )

    for c in creators[:15]:
        next_rank = get_next_rank(c.current_rank)
        progress = ""
        if next_rank:
            remaining = views_to_next_rank(c.current_rank, c.lifetime_views)
            progress = f"\n↗ {format_views(remaining)} to {next_rank.value}"

        embed.add_field(
            name=f"{get_rank_display(c.current_rank)}",
            value=f"**{c.name}**\n"
                  f"{format_views(c.lifetime_views)} views | {c.video_count} videos"
                  f"{progress}",
            inline=True
        )

    if len(creators) > 15:
        embed.set_footer(text=f"Showing 15 of {len(creators)} creators")

    await ctx.send(embed=embed)


@bot.command(name="setcreator")
async def set_creator(ctx: commands.Context, member: discord.Member = None, *, tiktok_name: str = None):
    """Link a Discord user to a TikTok creator name for auto role assignment."""
    if not member or not tiktok_name:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Parameters",
            f"Usage: `{COMMAND_PREFIX}setcreator @user tiktok_username`\n"
            f"Example: `{COMMAND_PREFIX}setcreator @John johndoe123`",
            COLOR_ERROR
        ))
        return

    bot.db.set_creator_discord_id(tiktok_name, member.id)

    # Immediately assign current rank role
    if ctx.guild:
        await update_creator_role(ctx.guild, tiktok_name, ctx.channel)

    profile = bot.db.get_or_create_creator(tiktok_name)

    await ctx.send(embed=create_embed(
        f"{EMOJI_SUCCESS} Creator Linked",
        f"**Discord:** {member.mention}\n"
        f"**TikTok:** {tiktok_name}\n"
        f"**Rank:** {get_rank_display(profile.current_rank)}\n"
        f"**Lifetime Views:** {format_views(profile.lifetime_views)}",
        COLOR_SUCCESS
    ))


@bot.command(name="ladder")
async def show_ladder(ctx: commands.Context):
    """Show the full earnings ladder with all rank tiers."""
    embed = create_embed(
        "💰 BonesMaxx Creator Earnings Ladder",
        "Earnings are calculated per qualifying video.\n"
        "Higher ranks unlock greater payout ceilings.\n"
        "Rank upgrades are permanent.",
        COLOR_INFO
    )

    for rank in RANK_ORDER:
        tiers = RANK_PAYOUT_TIERS[rank]
        threshold = RANK_THRESHOLDS[rank]

        tier_text = ""
        if threshold > 0:
            tier_text += f"_Unlocked at {format_views(threshold)} lifetime views_\n\n"

        running_total = 0
        for view_thresh, amount in tiers:
            running_total += amount
            if view_thresh == 20000:
                tier_text += f"${amount} at {format_views(view_thresh)} views\n"
            else:
                tier_text += f"+${amount} at {format_views(view_thresh)} views (${running_total} total)\n"

        tier_text += f"\n**Per-video cap: ${RANK_CAPS[rank]}**"

        embed.add_field(
            name=get_rank_display(rank),
            value=tier_text,
            inline=False
        )

    embed.set_footer(text="Lifetime views = total verified views across all content")
    await ctx.send(embed=embed)


@bot.command(name="giverole")
async def give_role(ctx: commands.Context, member: discord.Member = None, *, rank_name: str = None):
    """Manually assign a rank role to a Discord user."""
    if not member or not rank_name:
        rank_list = ", ".join([r.value for r in RANK_ORDER])
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Parameters",
            f"Usage: `{COMMAND_PREFIX}giverole @user rank_name`\n"
            f"Available ranks: `{rank_list}`\n"
            f"Example: `{COMMAND_PREFIX}giverole @John LTN`",
            COLOR_ERROR
        ))
        return

    # Find the rank
    rank_name_upper = rank_name.strip().upper()
    target_rank = None
    for r in RANK_ORDER:
        if r.value == rank_name_upper:
            target_rank = r
            break

    if not target_rank:
        rank_list = ", ".join([r.value for r in RANK_ORDER])
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Invalid Rank",
            f"**{rank_name}** is not a valid rank.\n"
            f"Available ranks: `{rank_list}`",
            COLOR_ERROR
        ))
        return

    # Get the role ID
    role_id = RANK_ROLES.get(target_rank)
    if not role_id:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Role Not Configured",
            f"No Discord role ID set for **{target_rank.value}**.",
            COLOR_ERROR
        ))
        return

    target_role = ctx.guild.get_role(role_id)
    if not target_role:
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Role Not Found",
            f"Could not find role with ID `{role_id}` in this server.",
            COLOR_ERROR
        ))
        return

    # Remove all existing rank roles
    all_rank_role_ids = {rid for rid in RANK_ROLES.values() if rid}
    roles_to_remove = [r for r in member.roles if r.id in all_rank_role_ids and r.id != role_id]
    if roles_to_remove:
        await member.remove_roles(*roles_to_remove, reason=f"Manual rank set to {target_rank.value}")

    # Add the new role
    if target_role not in member.roles:
        await member.add_roles(target_role, reason=f"Manual rank set by {ctx.author}")

    await ctx.message.add_reaction(EMOJI_SUCCESS)
    await ctx.send(embed=create_embed(
        f"{EMOJI_SUCCESS} Role Assigned",
        f"**User:** {member.mention}\n"
        f"**Rank:** {get_rank_display(target_rank)}",
        get_rank_color(target_rank)
    ))


# ============================================================================
# Error Handling
# ============================================================================

@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    """Global error handler."""
    if isinstance(error, commands.CommandNotFound):
        return

    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=create_embed(
            f"{EMOJI_ERROR} Missing Argument",
            f"Missing: `{error.param.name}`\nUse `{COMMAND_PREFIX}help` for usage.",
            COLOR_ERROR
        ))
        return

    logger.error(f"Command error in {ctx.command}: {error}", exc_info=error)
    await ctx.send(embed=create_embed(
        f"{EMOJI_ERROR} Error",
        str(error),
        COLOR_ERROR
    ))


# ============================================================================
# Main
# ============================================================================

def main():
    if not TOKEN:
        logger.error("DISCORD_BOT_TOKEN not found!")
        print("Error: DISCORD_BOT_TOKEN not found in .env file")
        return

    logger.info("Starting Payment Tracker Bot...")
    bot.run(TOKEN)


if __name__ == "__main__":
    main()
