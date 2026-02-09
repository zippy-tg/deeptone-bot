"""
Database operations for TikTok Creator Payment Tracker.
Uses SQLite for persistent storage of video submissions and payment records.
"""

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum

from utils import CreatorRank, determine_rank

logger = logging.getLogger(__name__)

DB_FILE = "creator_payments.db"


class PaymentStatus(Enum):
    """Payment status states."""
    PENDING = "pending"      # Waiting for 48hr eligibility
    ELIGIBLE = "eligible"    # Eligible but not paid
    PAID = "paid"           # Payment completed
    REJECTED = "rejected"   # Payment rejected


@dataclass
class ViewHistoryEntry:
    """Single view count history entry."""
    views: int
    date: str
    note: str = ""

    def to_dict(self) -> dict:
        return {"views": self.views, "date": self.date, "note": self.note}

    @classmethod
    def from_dict(cls, data: dict) -> "ViewHistoryEntry":
        return cls(views=data["views"], date=data["date"], note=data.get("note", ""))


@dataclass
class VideoRecord:
    """Represents a video submission record."""
    id: int
    video_id: str
    url: str
    creator_name: str
    view_count: int
    view_count_history: List[ViewHistoryEntry]
    date_posted: datetime
    date_eligible: datetime
    date_submitted: datetime
    base_payment: float
    bonus_amount: float
    total_payment: float
    needs_custom_bonus: bool
    payment_status: PaymentStatus
    rejection_reason: Optional[str]
    date_paid: Optional[datetime]
    notes: Optional[str]

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "VideoRecord":
        """Create a VideoRecord from a database row."""
        # Parse view history JSON
        history_json = row["view_count_history"] or "[]"
        history = [ViewHistoryEntry.from_dict(h) for h in json.loads(history_json)]

        # Parse dates
        date_posted = datetime.fromisoformat(row["date_posted"]) if row["date_posted"] else None
        date_eligible = datetime.fromisoformat(row["date_eligible"]) if row["date_eligible"] else None
        date_submitted = datetime.fromisoformat(row["date_submitted"])
        date_paid = datetime.fromisoformat(row["date_paid"]) if row["date_paid"] else None

        return cls(
            id=row["id"],
            video_id=row["video_id"],
            url=row["url"],
            creator_name=row["creator_name"],
            view_count=row["view_count"] or 0,
            view_count_history=history,
            date_posted=date_posted,
            date_eligible=date_eligible,
            date_submitted=date_submitted,
            base_payment=row["base_payment"] or 0,
            bonus_amount=row["bonus_amount"] or 0,
            total_payment=row["total_payment"] or 0,
            needs_custom_bonus=bool(row["needs_custom_bonus"]),
            payment_status=PaymentStatus(row["payment_status"] or "pending"),
            rejection_reason=row["rejection_reason"],
            date_paid=date_paid,
            notes=row["notes"]
        )

    def is_eligible(self) -> bool:
        """Check if video has passed 48hr eligibility window."""
        if not self.date_eligible:
            return False
        return datetime.now() >= self.date_eligible

    def hours_until_eligible(self) -> float:
        """Get hours remaining until eligible."""
        if not self.date_eligible:
            return 0
        delta = self.date_eligible - datetime.now()
        return max(0, delta.total_seconds() / 3600)


@dataclass
class CreatorProfile:
    """Creator profile with rank info."""
    name: str
    lifetime_views: int
    current_rank: CreatorRank
    discord_user_id: Optional[int]
    video_count: int
    total_paid: float
    unpaid_amount: float


@dataclass
class CreatorStats:
    """Statistics for a single creator."""
    name: str
    video_count: int
    total_views: int
    total_paid: float
    unpaid_amount: float


@dataclass
class OverallStats:
    """Overall payment statistics."""
    total_videos: int
    pending_count: int
    eligible_count: int
    paid_count: int
    rejected_count: int
    total_owed: float
    total_paid: float
    average_per_video: float
    highest_payout: float
    unique_creators: int
    top_earner_week: Optional[Tuple[str, float, int]]  # (name, amount, video_count)


class Database:
    """Handles all database operations for the payment tracker."""

    def __init__(self, db_file: str = DB_FILE):
        self.db_file = db_file
        self._init_db()

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()

    def _init_db(self):
        """Initialize database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT UNIQUE NOT NULL,
                    url TEXT NOT NULL,
                    creator_name TEXT NOT NULL,
                    view_count INTEGER DEFAULT 0,
                    view_count_history TEXT DEFAULT '[]',
                    date_posted TEXT,
                    date_eligible TEXT,
                    date_submitted TEXT NOT NULL,
                    base_payment REAL DEFAULT 0,
                    bonus_amount REAL DEFAULT 0,
                    total_payment REAL DEFAULT 0,
                    needs_custom_bonus INTEGER DEFAULT 0,
                    payment_status TEXT DEFAULT 'pending',
                    rejection_reason TEXT,
                    date_paid TEXT,
                    notes TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS creators (
                    creator_name TEXT PRIMARY KEY,
                    discord_user_id INTEGER,
                    current_rank TEXT DEFAULT 'SUB5',
                    lifetime_views INTEGER DEFAULT 0,
                    date_created TEXT NOT NULL
                )
            """)
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_video_id ON videos(video_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_creator_name ON videos(creator_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_payment_status ON videos(payment_status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_date_eligible ON videos(date_eligible)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_creator_discord ON creators(discord_user_id)")
            logger.info("Database initialized successfully")

    def check_duplicate(self, video_id: str) -> Optional[VideoRecord]:
        """Check if a video ID already exists."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,))
            row = cursor.fetchone()
            if row:
                return VideoRecord.from_row(row)
            return None

    def add_video(
        self,
        video_id: str,
        url: str,
        creator_name: str,
        view_count: int,
        date_posted: datetime,
        base_payment: float,
        bonus_amount: float,
        total_payment: float,
        needs_custom_bonus: bool,
        notes: Optional[str] = None
    ) -> VideoRecord:
        """Add a new video submission."""
        date_submitted = datetime.now()
        date_eligible = date_posted + timedelta(hours=48)

        # Determine initial status
        if datetime.now() >= date_eligible and view_count >= 20000:
            status = PaymentStatus.ELIGIBLE
        else:
            status = PaymentStatus.PENDING

        # Create initial view history
        history = [ViewHistoryEntry(
            views=view_count,
            date=date_submitted.strftime("%Y-%m-%d"),
            note="Initial submission"
        )]
        history_json = json.dumps([h.to_dict() for h in history])

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO videos (
                    video_id, url, creator_name, view_count, view_count_history,
                    date_posted, date_eligible, date_submitted,
                    base_payment, bonus_amount, total_payment, needs_custom_bonus,
                    payment_status, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                video_id, url, creator_name, view_count, history_json,
                date_posted.isoformat(), date_eligible.isoformat(), date_submitted.isoformat(),
                base_payment, bonus_amount, total_payment, int(needs_custom_bonus),
                status.value, notes
            ))

            cursor.execute("SELECT * FROM videos WHERE id = ?", (cursor.lastrowid,))
            row = cursor.fetchone()
            logger.info(f"Added video {video_id} - Creator: {creator_name}, Views: {view_count}")
            return VideoRecord.from_row(row)

    def get_video_by_id(self, video_id: str) -> Optional[VideoRecord]:
        """Get a video by its TikTok video ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,))
            row = cursor.fetchone()
            if row:
                return VideoRecord.from_row(row)
            return None

    def update_views(
        self,
        video_id: str,
        new_views: int,
        base_payment: float,
        bonus_amount: float,
        total_payment: float,
        needs_custom_bonus: bool
    ) -> Optional[VideoRecord]:
        """Update view count and recalculate payment."""
        existing = self.get_video_by_id(video_id)
        if not existing:
            return None

        # Update history
        history = existing.view_count_history
        history.append(ViewHistoryEntry(
            views=new_views,
            date=datetime.now().strftime("%Y-%m-%d"),
            note="Updated"
        ))
        history_json = json.dumps([h.to_dict() for h in history])

        # Update status if now eligible
        status = existing.payment_status
        if status == PaymentStatus.PENDING:
            if existing.is_eligible() and new_views >= 20000:
                status = PaymentStatus.ELIGIBLE

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE videos SET
                    view_count = ?,
                    view_count_history = ?,
                    base_payment = ?,
                    bonus_amount = ?,
                    total_payment = ?,
                    needs_custom_bonus = ?,
                    payment_status = ?
                WHERE video_id = ?
            """, (
                new_views, history_json, base_payment, bonus_amount,
                total_payment, int(needs_custom_bonus), status.value, video_id
            ))

            cursor.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,))
            row = cursor.fetchone()
            logger.info(f"Updated views for {video_id}: {existing.view_count} -> {new_views}")
            return VideoRecord.from_row(row)

    def mark_paid(self, video_id: str) -> Optional[VideoRecord]:
        """Mark a video as paid."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE videos SET
                    payment_status = ?,
                    date_paid = ?
                WHERE video_id = ?
            """, (PaymentStatus.PAID.value, datetime.now().isoformat(), video_id))

            if cursor.rowcount == 0:
                return None

            cursor.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,))
            row = cursor.fetchone()
            logger.info(f"Marked {video_id} as paid")
            return VideoRecord.from_row(row)

    def reject_payment(self, video_id: str, reason: str) -> Optional[VideoRecord]:
        """Reject a video payment."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE videos SET
                    payment_status = ?,
                    rejection_reason = ?
                WHERE video_id = ?
            """, (PaymentStatus.REJECTED.value, reason, video_id))

            if cursor.rowcount == 0:
                return None

            cursor.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,))
            row = cursor.fetchone()
            logger.info(f"Rejected {video_id}: {reason}")
            return VideoRecord.from_row(row)

    def delete_video(self, video_id: str) -> bool:
        """Delete a video record."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM videos WHERE video_id = ?", (video_id,))
            success = cursor.rowcount > 0
            if success:
                logger.info(f"Deleted video {video_id}")
            return success

    def get_pending_videos(self) -> List[VideoRecord]:
        """Get videos waiting for 48hr eligibility."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM videos
                WHERE payment_status = 'pending'
                ORDER BY date_eligible ASC
            """)
            return [VideoRecord.from_row(row) for row in cursor.fetchall()]

    def get_eligible_videos(self) -> List[VideoRecord]:
        """Get videos eligible for payment (passed 48hr, 20k+ views)."""
        now = datetime.now().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM videos
                WHERE payment_status = 'eligible'
                    AND date_eligible <= ?
                    AND view_count >= 20000
                ORDER BY total_payment DESC
            """, (now,))
            return [VideoRecord.from_row(row) for row in cursor.fetchall()]

    def get_unpaid_videos(self) -> List[VideoRecord]:
        """Get eligible videos not yet paid."""
        return self.get_eligible_videos()

    def get_paid_videos(self, limit: int = None) -> List[VideoRecord]:
        """Get paid videos."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT * FROM videos
                WHERE payment_status = 'paid'
                ORDER BY date_paid DESC
            """
            if limit:
                query += f" LIMIT {limit}"
            cursor.execute(query)
            return [VideoRecord.from_row(row) for row in cursor.fetchall()]

    def get_creator_videos(self, creator_name: str) -> List[VideoRecord]:
        """Get all videos for a creator."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM videos
                WHERE LOWER(creator_name) = LOWER(?)
                ORDER BY date_submitted DESC
            """, (creator_name,))
            return [VideoRecord.from_row(row) for row in cursor.fetchall()]

    def get_recent_videos(self, limit: int = 10) -> List[VideoRecord]:
        """Get recent video submissions."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM videos
                ORDER BY date_submitted DESC
                LIMIT ?
            """, (limit,))
            return [VideoRecord.from_row(row) for row in cursor.fetchall()]

    def get_all_videos(self) -> List[VideoRecord]:
        """Get all videos."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM videos ORDER BY date_submitted DESC")
            return [VideoRecord.from_row(row) for row in cursor.fetchall()]

    def get_weekly_report(self) -> List[Dict[str, Any]]:
        """Get weekly payout report grouped by creator."""
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    creator_name,
                    COUNT(*) as video_count,
                    SUM(view_count) as total_views,
                    SUM(base_payment) as total_base,
                    SUM(bonus_amount) as total_bonus,
                    SUM(total_payment) as total_owed
                FROM videos
                WHERE payment_status IN ('eligible', 'pending')
                    AND date_submitted >= ?
                GROUP BY creator_name
                ORDER BY total_owed DESC
            """, (week_ago,))

            return [
                {
                    "creator": row["creator_name"],
                    "videos": row["video_count"],
                    "total_views": row["total_views"],
                    "base_pay": row["total_base"],
                    "bonuses": row["total_bonus"],
                    "total_owed": row["total_owed"]
                }
                for row in cursor.fetchall()
            ]

    def get_stats(self) -> OverallStats:
        """Get overall statistics."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Count by status
            cursor.execute("""
                SELECT
                    payment_status,
                    COUNT(*) as count,
                    SUM(total_payment) as total
                FROM videos
                GROUP BY payment_status
            """)
            status_counts = {row["payment_status"]: (row["count"], row["total"] or 0)
                           for row in cursor.fetchall()}

            pending_count = status_counts.get("pending", (0, 0))[0]
            eligible_count = status_counts.get("eligible", (0, 0))[0]
            paid_count = status_counts.get("paid", (0, 0))[0]
            rejected_count = status_counts.get("rejected", (0, 0))[0]

            total_owed = status_counts.get("eligible", (0, 0))[1]
            total_paid = status_counts.get("paid", (0, 0))[1]

            # Total and average
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    AVG(total_payment) as avg_payment,
                    MAX(total_payment) as max_payment,
                    COUNT(DISTINCT creator_name) as creators
                FROM videos
                WHERE payment_status != 'rejected'
            """)
            row = cursor.fetchone()
            total_videos = row["total"]
            avg_payment = row["avg_payment"] or 0
            max_payment = row["max_payment"] or 0
            unique_creators = row["creators"]

            # Top earner this week
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            cursor.execute("""
                SELECT
                    creator_name,
                    SUM(total_payment) as week_total,
                    COUNT(*) as video_count
                FROM videos
                WHERE date_submitted >= ?
                    AND payment_status != 'rejected'
                GROUP BY creator_name
                ORDER BY week_total DESC
                LIMIT 1
            """, (week_ago,))
            top_row = cursor.fetchone()
            top_earner = None
            if top_row and top_row["week_total"]:
                top_earner = (top_row["creator_name"], top_row["week_total"], top_row["video_count"])

            return OverallStats(
                total_videos=total_videos,
                pending_count=pending_count,
                eligible_count=eligible_count,
                paid_count=paid_count,
                rejected_count=rejected_count,
                total_owed=total_owed,
                total_paid=total_paid,
                average_per_video=avg_payment,
                highest_payout=max_payment,
                unique_creators=unique_creators,
                top_earner_week=top_earner
            )

    def update_pending_to_eligible(self) -> int:
        """Update videos that have passed their eligibility date."""
        now = datetime.now().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE videos SET payment_status = 'eligible'
                WHERE payment_status = 'pending'
                    AND date_eligible <= ?
                    AND view_count >= 20000
            """, (now,))
            count = cursor.rowcount
            if count > 0:
                logger.info(f"Updated {count} videos from pending to eligible")
            return count

    # ========================================================================
    # Creator Rank Methods
    # ========================================================================

    def get_or_create_creator(self, creator_name: str) -> CreatorProfile:
        """Get creator profile, creating if needed. Recalculates lifetime views."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Calculate lifetime views from all non-rejected videos
            cursor.execute("""
                SELECT COALESCE(SUM(view_count), 0) as total_views,
                       COUNT(*) as video_count,
                       COALESCE(SUM(CASE WHEN payment_status = 'paid' THEN total_payment ELSE 0 END), 0) as total_paid,
                       COALESCE(SUM(CASE WHEN payment_status = 'eligible' THEN total_payment ELSE 0 END), 0) as unpaid
                FROM videos
                WHERE LOWER(creator_name) = LOWER(?)
                  AND payment_status != 'rejected'
            """, (creator_name,))
            row = cursor.fetchone()
            lifetime_views = row["total_views"]
            video_count = row["video_count"]
            total_paid = row["total_paid"]
            unpaid_amount = row["unpaid"]

            # Determine rank
            rank = determine_rank(lifetime_views)

            # Upsert creator
            cursor.execute("""
                INSERT INTO creators (creator_name, current_rank, lifetime_views, date_created)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(creator_name) DO UPDATE SET
                    current_rank = ?,
                    lifetime_views = ?
            """, (creator_name, rank.value, lifetime_views, datetime.now().isoformat(),
                  rank.value, lifetime_views))

            # Get discord user id
            cursor.execute("SELECT discord_user_id FROM creators WHERE creator_name = ?", (creator_name,))
            creator_row = cursor.fetchone()
            discord_user_id = creator_row["discord_user_id"] if creator_row else None

            return CreatorProfile(
                name=creator_name,
                lifetime_views=lifetime_views,
                current_rank=rank,
                discord_user_id=discord_user_id,
                video_count=video_count,
                total_paid=total_paid,
                unpaid_amount=unpaid_amount
            )

    def set_creator_discord_id(self, creator_name: str, discord_user_id: int) -> bool:
        """Link a Discord user to a creator name."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Ensure creator exists
            cursor.execute("""
                INSERT INTO creators (creator_name, discord_user_id, date_created)
                VALUES (?, ?, ?)
                ON CONFLICT(creator_name) DO UPDATE SET discord_user_id = ?
            """, (creator_name, discord_user_id, datetime.now().isoformat(), discord_user_id))
            return cursor.rowcount > 0

    def get_creator_by_discord_id(self, discord_user_id: int) -> Optional[str]:
        """Get creator name linked to a Discord user ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT creator_name FROM creators WHERE discord_user_id = ?", (discord_user_id,))
            row = cursor.fetchone()
            return row["creator_name"] if row else None

    def get_all_creators_with_ranks(self) -> List[CreatorProfile]:
        """Get all creators with their rank info."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    v.creator_name,
                    COALESCE(SUM(v.view_count), 0) as lifetime_views,
                    COUNT(*) as video_count,
                    COALESCE(SUM(CASE WHEN v.payment_status = 'paid' THEN v.total_payment ELSE 0 END), 0) as total_paid,
                    COALESCE(SUM(CASE WHEN v.payment_status = 'eligible' THEN v.total_payment ELSE 0 END), 0) as unpaid,
                    c.discord_user_id
                FROM videos v
                LEFT JOIN creators c ON LOWER(v.creator_name) = LOWER(c.creator_name)
                WHERE v.payment_status != 'rejected'
                GROUP BY v.creator_name
                ORDER BY lifetime_views DESC
            """)
            results = []
            for row in cursor.fetchall():
                rank = determine_rank(row["lifetime_views"])
                results.append(CreatorProfile(
                    name=row["creator_name"],
                    lifetime_views=row["lifetime_views"],
                    current_rank=rank,
                    discord_user_id=row["discord_user_id"],
                    video_count=row["video_count"],
                    total_paid=row["total_paid"],
                    unpaid_amount=row["unpaid"]
                ))
            return results

    def export_to_csv_data(self) -> List[Tuple]:
        """Get all data for CSV export."""
        videos = self.get_all_videos()
        return [
            (
                v.date_submitted.strftime("%Y-%m-%d"),
                v.creator_name,
                v.video_id,
                v.url,
                v.view_count,
                f"{v.base_payment:.2f}",
                f"{v.bonus_amount:.2f}",
                f"{v.total_payment:.2f}",
                v.payment_status.value,
                v.date_posted.strftime("%Y-%m-%d") if v.date_posted else "",
                v.date_paid.strftime("%Y-%m-%d") if v.date_paid else "",
                v.notes or ""
            )
            for v in videos
        ]
