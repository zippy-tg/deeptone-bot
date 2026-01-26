"""
Utility functions for TikTok Creator Payment Tracker.
Handles URL parsing, TikTok scraping, payment calculation, and other helpers.
"""

import re
import logging
from typing import Optional, Tuple, NamedTuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import aiohttp
import requests
from bs4 import BeautifulSoup
import json

logger = logging.getLogger(__name__)

# TikTok URL patterns
TIKTOK_PATTERNS = {
    "standard": re.compile(
        r"(?:https?://)?(?:www\.)?tiktok\.com/@[\w.-]+/video/(\d+)",
        re.IGNORECASE
    ),
    "vm_short": re.compile(
        r"(?:https?://)?vm\.tiktok\.com/([\w-]+)/?",
        re.IGNORECASE
    ),
    "t_short": re.compile(
        r"(?:https?://)?(?:www\.)?tiktok\.com/t/([\w-]+)/?",
        re.IGNORECASE
    ),
    "mobile": re.compile(
        r"(?:https?://)?(?:www\.)?tiktok\.com/.*[?&]video_id=(\d+)",
        re.IGNORECASE
    ),
}

# User agent for scraping
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


@dataclass
class TikTokVideoData:
    """Scraped TikTok video data."""
    views: Optional[int] = None
    date_posted: Optional[datetime] = None
    username: Optional[str] = None
    description: Optional[str] = None
    error: Optional[str] = None


@dataclass
class PaymentCalculation:
    """Payment calculation result."""
    base_payment: float
    bonus_amount: float
    total_payment: float
    needs_custom_bonus: bool
    tiers: int
    eligible: bool
    bonuses: list  # List of (threshold, amount) tuples


class TikTokURLParser:
    """Handles parsing and resolving TikTok URLs."""

    USERNAME_PATTERN = re.compile(
        r"(?:https?://)?(?:www\.)?tiktok\.com/@([\w.-]+)",
        re.IGNORECASE
    )

    @staticmethod
    def extract_username(url: str) -> Optional[str]:
        """Extract username from TikTok URL."""
        url = url.strip()
        match = TikTokURLParser.USERNAME_PATTERN.search(url)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def extract_video_id(url: str) -> Optional[str]:
        """Extract video ID from TikTok URL."""
        url = url.strip()

        match = TIKTOK_PATTERNS["standard"].search(url)
        if match:
            return match.group(1)

        match = TIKTOK_PATTERNS["mobile"].search(url)
        if match:
            return match.group(1)

        for pattern_name in ["vm_short", "t_short"]:
            match = TIKTOK_PATTERNS[pattern_name].search(url)
            if match:
                return f"short_{match.group(1)}"

        return None

    @staticmethod
    def is_valid_tiktok_url(url: str) -> bool:
        """Check if URL is valid TikTok URL."""
        url = url.strip().lower()
        return "tiktok.com" in url or "vm.tiktok.com" in url

    @staticmethod
    async def resolve_short_url(url: str) -> Optional[str]:
        """Resolve short TikTok URL to full URL."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.head(
                    url,
                    allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers={"User-Agent": USER_AGENT}
                ) as response:
                    return str(response.url)
        except Exception as e:
            logger.error(f"Failed to resolve short URL {url}: {e}")
            return None

    @classmethod
    async def parse_url(cls, url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse TikTok URL and return (video_id, normalized_url, username)."""
        if not cls.is_valid_tiktok_url(url):
            return None, None, None

        is_short_url = "vm.tiktok.com" in url.lower() or "/t/" in url.lower()

        if is_short_url:
            resolved_url = await cls.resolve_short_url(url)
            if resolved_url:
                video_id = cls.extract_video_id(resolved_url)
                username = cls.extract_username(resolved_url)
                if video_id and not video_id.startswith("short_"):
                    return video_id, resolved_url, username
            video_id = cls.extract_video_id(url)
            return video_id, url, None

        video_id = cls.extract_video_id(url)
        username = cls.extract_username(url)
        return video_id, url, username


class TikTokScraper:
    """Scrapes video data from TikTok."""

    @staticmethod
    def parse_view_count(text: str) -> Optional[int]:
        """Parse view count from text like '45.2K', '1.2M', '500'."""
        if not text:
            return None

        text = text.strip().upper().replace(",", "")

        # Match patterns like "45.2K", "1.2M", "500"
        match = re.search(r"([\d.]+)\s*([KMB])?", text)
        if not match:
            return None

        try:
            num = float(match.group(1))
            unit = match.group(2)

            if unit == "K":
                num *= 1000
            elif unit == "M":
                num *= 1000000
            elif unit == "B":
                num *= 1000000000

            return int(num)
        except ValueError:
            return None

    @staticmethod
    def parse_date(text: str) -> Optional[datetime]:
        """Parse date from TikTok format."""
        if not text:
            return None

        text = text.strip()

        # Try various date formats
        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%m-%d",  # Current year assumed
            "%m/%d",
            "%b %d, %Y",
            "%B %d, %Y",
            "%d %b %Y",
            "%d %B %Y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(text, fmt)
                # If no year in format, assume current year
                if dt.year == 1900:
                    dt = dt.replace(year=datetime.now().year)
                return dt
            except ValueError:
                continue

        # Handle relative dates like "1d ago", "2w ago", "3h ago"
        relative_match = re.search(r"(\d+)\s*([hdwm])", text.lower())
        if relative_match:
            num = int(relative_match.group(1))
            unit = relative_match.group(2)

            now = datetime.now()
            if unit == "h":
                return now - timedelta(hours=num)
            elif unit == "d":
                return now - timedelta(days=num)
            elif unit == "w":
                return now - timedelta(weeks=num)
            elif unit == "m":
                return now - timedelta(days=num * 30)

        return None

    @classmethod
    def scrape_video(cls, url: str) -> TikTokVideoData:
        """Scrape video data from TikTok URL."""
        try:
            headers = {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            }

            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")
            data = TikTokVideoData()

            # Extract username from URL
            data.username = TikTokURLParser.extract_username(url)

            # Try to find JSON-LD data (most reliable)
            script_tags = soup.find_all("script", type="application/ld+json")
            for script in script_tags:
                try:
                    json_data = json.loads(script.string)
                    if isinstance(json_data, dict):
                        # Look for interactionStatistic (views)
                        if "interactionStatistic" in json_data:
                            for stat in json_data.get("interactionStatistic", []):
                                if stat.get("interactionType", {}).get("@type") == "WatchAction":
                                    data.views = int(stat.get("userInteractionCount", 0))

                        # Look for upload date
                        if "uploadDate" in json_data:
                            data.date_posted = cls.parse_date(json_data["uploadDate"])

                        # Description
                        if "description" in json_data:
                            data.description = json_data["description"]
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue

            # Fallback: Try meta tags
            if not data.views:
                # Look in og:description or other meta
                og_desc = soup.find("meta", property="og:description")
                if og_desc:
                    content = og_desc.get("content", "")
                    # Parse "45.2K Likes, 500 Comments, 1.2M views"
                    views_match = re.search(r"([\d.]+[KMB]?)\s*(?:views|plays)", content, re.IGNORECASE)
                    if views_match:
                        data.views = cls.parse_view_count(views_match.group(1))

            # Try SIGI_STATE data (TikTok's internal state)
            sigi_script = soup.find("script", id="SIGI_STATE")
            if sigi_script and sigi_script.string:
                try:
                    sigi_data = json.loads(sigi_script.string)
                    # Navigate to video data
                    item_module = sigi_data.get("ItemModule", {})
                    for video_id, video_data in item_module.items():
                        if "stats" in video_data:
                            stats = video_data["stats"]
                            if not data.views and "playCount" in stats:
                                data.views = int(stats["playCount"])

                        if not data.date_posted and "createTime" in video_data:
                            timestamp = int(video_data["createTime"])
                            data.date_posted = datetime.fromtimestamp(timestamp)

                        if not data.username and "author" in video_data:
                            data.username = video_data["author"]
                        break
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass

            # Try __UNIVERSAL_DATA_FOR_REHYDRATION__
            universal_script = soup.find("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__")
            if universal_script and universal_script.string:
                try:
                    uni_data = json.loads(universal_script.string)
                    default_scope = uni_data.get("__DEFAULT_SCOPE__", {})
                    video_detail = default_scope.get("webapp.video-detail", {})
                    item_info = video_detail.get("itemInfo", {}).get("itemStruct", {})

                    if not data.views and "stats" in item_info:
                        data.views = int(item_info["stats"].get("playCount", 0))

                    if not data.date_posted and "createTime" in item_info:
                        timestamp = int(item_info["createTime"])
                        data.date_posted = datetime.fromtimestamp(timestamp)

                    if not data.username and "author" in item_info:
                        data.username = item_info["author"].get("uniqueId")
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    pass

            return data

        except requests.RequestException as e:
            logger.error(f"Failed to scrape TikTok URL {url}: {e}")
            return TikTokVideoData(error=f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error scraping {url}: {e}")
            return TikTokVideoData(error=f"Scraping error: {str(e)}")


def calculate_payment(views: int) -> PaymentCalculation:
    """
    Calculate payment based on view count.

    Payment Structure (New Creators):
    - Base: FLAT $20 for any video with 20k+ views
    - Bonuses are ONE-TIME add-ons (not multipliers):
      - 40k views: +$5
      - 75k views: +$5
      - 150k+ views: Custom bonus (based on conversion)

    Example: 75,000 views = $20 (base) + $5 (40k) + $5 (75k) = $30 total
    """
    if views < 20000:
        return PaymentCalculation(
            base_payment=0,
            bonus_amount=0,
            total_payment=0,
            needs_custom_bonus=False,
            tiers=0,
            eligible=False,
            bonuses=[]
        )

    # Base payment: FLAT $20 for qualifying (20k+)
    base_payment = 20

    # Bonuses are one-time add-ons per milestone
    bonuses = []
    bonus_amount = 0

    if views >= 40000:
        bonuses.append((40000, 5))
        bonus_amount += 5

    if views >= 75000:
        bonuses.append((75000, 5))
        bonus_amount += 5

    needs_custom_bonus = views >= 150000

    total_payment = base_payment + bonus_amount

    return PaymentCalculation(
        base_payment=base_payment,
        bonus_amount=bonus_amount,
        total_payment=total_payment,
        needs_custom_bonus=needs_custom_bonus,
        tiers=1,  # Flat rate (not per 20k)
        eligible=True,
        bonuses=bonuses
    )


def format_views(views: int) -> str:
    """Format view count for display (e.g., 45,273 or 1.2M)."""
    if views >= 1000000:
        return f"{views / 1000000:.1f}M"
    elif views >= 1000:
        return f"{views / 1000:.1f}K"
    return f"{views:,}"


def format_amount(amount: float, currency: str = "USD") -> str:
    """Format amount with currency symbol."""
    symbols = {"USD": "$", "EUR": "€", "GBP": "£"}
    symbol = symbols.get(currency, "$")
    return f"{symbol}{amount:,.2f}"


def format_date(dt: datetime, include_time: bool = False) -> str:
    """Format datetime for display."""
    if not dt:
        return "Unknown"
    if include_time:
        return dt.strftime("%B %d, %Y at %H:%M")
    return dt.strftime("%B %d, %Y")


def format_date_short(dt: datetime) -> str:
    """Format datetime in short format."""
    if not dt:
        return "Unknown"
    return dt.strftime("%b %d")


def parse_views_input(text: str) -> Optional[int]:
    """Parse user input for view count."""
    if not text:
        return None

    text = text.strip().upper().replace(",", "").replace(" ", "")

    # Handle K/M suffixes
    match = re.match(r"^([\d.]+)([KM])?$", text)
    if match:
        try:
            num = float(match.group(1))
            unit = match.group(2)
            if unit == "K":
                num *= 1000
            elif unit == "M":
                num *= 1000000
            return int(num)
        except ValueError:
            return None

    # Plain number
    try:
        return int(text)
    except ValueError:
        return None


def parse_date_input(text: str) -> Optional[datetime]:
    """Parse user input for date."""
    if not text:
        return None

    text = text.strip().lower()

    if text in ["today", "now"]:
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if text == "yesterday":
        return (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Try various formats
    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%m-%d-%Y",
        "%m/%d/%Y",
        "%b %d",
        "%B %d",
        "%d %b",
        "%d %B",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt
        except ValueError:
            continue

    # Handle "X days ago" format
    days_match = re.match(r"(\d+)\s*d(?:ays?)?\s*ago", text)
    if days_match:
        days = int(days_match.group(1))
        return (datetime.now() - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

    return None


def format_video_id_display(video_id: str) -> str:
    """Format video ID for display."""
    if video_id.startswith("short_"):
        return f"{video_id[6:]} (shortcode)"
    return video_id


def format_hours(hours: float) -> str:
    """Format hours into human readable string."""
    if hours < 1:
        return f"{int(hours * 60)} minutes"
    elif hours < 24:
        return f"{hours:.1f} hours"
    else:
        days = hours / 24
        return f"{days:.1f} days"


# Status emoji mapping
STATUS_EMOJI = {
    "pending": "⏳",
    "eligible": "✅",
    "paid": "💸",
    "rejected": "❌"
}


def get_status_emoji(status: str) -> str:
    """Get emoji for payment status."""
    return STATUS_EMOJI.get(status, "❓")
