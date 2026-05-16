"""
Extracts a deterministic, reproducible random sample of 1 000 users
from the ``users`` table, enriches them with their 5 most recent tweets
and a few computed features, and exports a CSV ready for manual
annotation.

Reproducibility:
    The random ordering is derived from ``MD5(user_id || seed)`` — same
    user set + same seed = same sample, on any machine, any PostgreSQL
    version.

Usage:
    python -m src.ml.sample_users

Output:
    ``data/processed/labeling_sample.csv``
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime

from psycopg2.extras import RealDictCursor

from src.db import get_db_connection

# ---------------------------------------------------------------------------
# Configurable constants
# ---------------------------------------------------------------------------
SAMPLE_SIZE = 1000
RANDOM_SEED = "if29-bot-detection-2025"  # change to get a different sample
TWEETS_PER_USER = 5  # how many recent tweets to include
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed")
OUTPUT_FILE = "labeling_sample.csv"

# ---------------------------------------------------------------------------
# CSV columns (order matters)
# ---------------------------------------------------------------------------
CSV_COLUMNS = [
    # --- identity ---
    "user_id",
    "screen_name",
    "name",
    # --- metadata ---
    "description",
    "location",
    "followers_count",
    "friends_count",
    "listed_count",
    "favourites_count",
    "statuses_count",
    "verified",
    "account_created_at",
    "default_profile",
    "default_profile_image",
    # --- dataset stats ---
    "tweet_count_in_dataset",
    "first_seen_at",
    "last_seen_at",
    # --- computed features ---
    "followers_friends_ratio",
    "tweets_per_day_since_creation",
    "dataset_coverage_days",
    "tweets_per_day_in_dataset",
    # --- sample tweets ---
    "sample_tweets",
    # --- annotation columns (leave empty) ---
    "label",  # 0 = human, 1 = bot
    "notes",
]

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def _ensure_output_dir() -> None:
    """Create the output directory if it does not exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def sample_users() -> str:
    """Run the full sampling pipeline.

    Returns:
        Path to the generated CSV file.
    """
    _ensure_output_dir()

    logger.info("Connecting to database ...")

    with get_db_connection() as conn:
        # 1. Pick 1 000 users deterministically
        user_ids = _pick_random_users(conn)
        logger.info("Selected %d user IDs for annotation.", len(user_ids))

        # 2. Fetch full user rows for those IDs
        users = _fetch_users(conn, user_ids)
        logger.info("Fetched %d user profiles.", len(users))

        # 3. Fetch recent tweets for each user
        tweets_map = _fetch_recent_tweets(conn, user_ids)
        logger.info("Fetched tweets for %d users.", len(tweets_map))

        # 4. Assemble rows
        rows = _assemble_rows(users, tweets_map)
        logger.info("Assembled %d rows.", len(rows))

        # 5. Write CSV
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        _write_csv(output_path, rows)
        logger.info("CSV written to %s", output_path)

        return output_path


# ---------------------------------------------------------------------------
# Step 1 — deterministic random selection
# ---------------------------------------------------------------------------


def _pick_random_users(conn) -> list[int]:
    """Select ``SAMPLE_SIZE`` user IDs in a reproducible pseudo-random order.

    Uses ``MD5(user_id || seed)`` so that the same dataset + same seed
    always yields the same set of users, independent of PostgreSQL
    version or ``random()`` state.

    Returns:
        List of user IDs in deterministic pseudo-random order.
    """
    query = """
        SELECT user_id
        FROM users
        ORDER BY MD5(user_id::text || %s)
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (RANDOM_SEED, SAMPLE_SIZE))
        return [row[0] for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Step 2 — fetch user rows
# ---------------------------------------------------------------------------


def _fetch_users(conn, user_ids: list[int]) -> dict[int, dict]:
    """Fetch full user rows for a list of user IDs.

    Args:
        conn: Open psycopg2 database connection.
        user_ids: List of Twitter user IDs to fetch.

    Returns:
        Mapping of user_id → row dict.
    """
    if not user_ids:
        return {}

    query = """
        SELECT *
        FROM users
        WHERE user_id = ANY(%s)
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (user_ids,))
        return {row["user_id"]: dict(row) for row in cur}


# ---------------------------------------------------------------------------
# Step 3 — fetch recent tweets
# ---------------------------------------------------------------------------


def _fetch_recent_tweets(conn, user_ids: list[int]) -> dict[int, list[str]]:
    """For each user, return up to ``TWEETS_PER_USER`` most recent tweet texts.

    Extracts the user ID from ``raw_data`` JSONB using the expression
    index ``idx_tweets_user_date`` (see ``init.sql``) for performance.

    Args:
        conn: Open psycopg2 database connection.
        user_ids: List of Twitter user IDs.

    Returns:
        Mapping of user_id → list of tweet texts (most recent first).
    """
    if not user_ids:
        return {}

    # Outer query references columns from the subquery alias "sub":
    # _uid, tweet_text, rn — NOT raw_data.
    query = """
        SELECT
            _uid AS user_id,
            tweet_text
        FROM (
            SELECT
                (raw_data->'user'->>'id')::BIGINT AS _uid,
                tweet_text,
                created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY (raw_data->'user'->>'id')::BIGINT
                    ORDER BY created_at DESC
                ) AS rn
            FROM tweets
            WHERE (raw_data->'user'->>'id')::BIGINT = ANY(%s)
        ) sub
        WHERE rn <= %s
        ORDER BY _uid, rn
    """
    with conn.cursor() as cur:
        cur.execute(query, (user_ids, TWEETS_PER_USER))
        rows = cur.fetchall()

    tweets_map: dict[int, list[str]] = {uid: [] for uid in user_ids}
    for uid, text in rows:
        tweets_map[uid].append(text)
    return tweets_map


# ---------------------------------------------------------------------------
# Step 4 — assemble CSV rows with computed features
# ---------------------------------------------------------------------------


def _assemble_rows(
    users: dict[int, dict],
    tweets_map: dict[int, list[str]],
) -> list[dict]:
    """Merge user info, computed features, and sample tweets into CSV rows.

    Args:
        users: Mapping of user_id → user row dict from the ``users`` table.
        tweets_map: Mapping of user_id → list of recent tweet texts.

    Returns:
        List of dicts, one per user, ready for CSV export.
    """
    rows = []

    for uid, user in users.items():
        # --- computed features ---
        ff_ratio = _safe_ratio(
            user.get("friends_count") or 0,
            user.get("followers_count") or 0,
        )

        tweets_per_day_since_creation = _tweets_per_day(
            user.get("statuses_count") or 0,
            user.get("created_at"),
            user.get("last_tweet_at"),
        )

        dataset_coverage = _days_between(
            user.get("first_seen_at"),
            user.get("last_seen_at"),
        )

        tweets_per_day_in_dataset = _tweets_per_day(
            user.get("tweet_count") or 0,
            user.get("first_seen_at"),
            user.get("last_seen_at"),
        )

        # --- sample tweets (separated so they stay in one cell) ---
        tweets = tweets_map.get(uid, [])
        sample_tweets = " ||| ".join(
            t.replace("\n", " ").replace("\r", "") for t in tweets if t
        )

        row = {
            "user_id": uid,
            "screen_name": user.get("screen_name"),
            "name": user.get("name"),
            "description": user.get("description"),
            "location": user.get("location"),
            "followers_count": user.get("followers_count"),
            "friends_count": user.get("friends_count"),
            "listed_count": user.get("listed_count"),
            "favourites_count": user.get("favourites_count"),
            "statuses_count": user.get("statuses_count"),
            "verified": user.get("verified"),
            "account_created_at": _fmt_ts(user.get("created_at")),
            "default_profile": user.get("default_profile"),
            "default_profile_image": user.get("default_profile_image"),
            "tweet_count_in_dataset": user.get("tweet_count"),
            "first_seen_at": _fmt_ts(user.get("first_seen_at")),
            "last_seen_at": _fmt_ts(user.get("last_seen_at")),
            "followers_friends_ratio": round(ff_ratio, 4),
            "tweets_per_day_since_creation": round(tweets_per_day_since_creation, 2),
            "dataset_coverage_days": dataset_coverage,
            "tweets_per_day_in_dataset": round(tweets_per_day_in_dataset, 2),
            "sample_tweets": sample_tweets,
            "label": "",  # to be filled by annotator
            "notes": "",  # to be filled by annotator
        }
        rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Step 5 — write CSV
# ---------------------------------------------------------------------------


def _write_csv(path: str, rows: list[dict]) -> None:
    """Write the list of dicts as a CSV file.

    Args:
        path: Destination file path.
        rows: List of dicts with keys matching ``CSV_COLUMNS``.
    """
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _safe_ratio(num: int, den: int) -> float:
    """Return num/den, or 0 if den is 0.

    Args:
        num: Numerator.
        den: Denominator.

    Returns:
        The ratio, or 0.0 if the denominator is zero.
    """
    return num / den if den else 0.0


def _tweets_per_day(count: int, start, end) -> float:
    """Compute average tweets per day between two dates.

    Args:
        count: Total number of tweets.
        start: Start timestamp (datetime or ISO string).
        end: End timestamp (datetime or ISO string).

    Returns:
        Average tweets per day, or 0.0 if the period is zero or undefined.
    """
    days = _days_between(start, end)
    return count / days if days and days > 0 else 0.0


def _days_between(start, end) -> int | None:
    """Return the number of days between two timestamps.

    Args:
        start: Start timestamp (datetime or ISO string).
        end: End timestamp (datetime or ISO string).

    Returns:
        Number of days (≥ 0), or None if either bound is missing.
    """
    if start is None or end is None:
        return None
    s = _to_datetime(start)
    e = _to_datetime(end)
    return max(0, (e - s).days)


def _to_datetime(val):
    """Coerce a value to datetime if it isn't already.

    Args:
        val: A datetime object or ISO-formatted string.

    Returns:
        A datetime, or None if parsing fails.
    """
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            return None
    return None


def _fmt_ts(val) -> str:
    """Format a timestamp as an ISO string.

    Args:
        val: A datetime object or ISO-formatted string.

    Returns:
        ISO-formatted string, or an empty string if the value is None.
    """
    dt = _to_datetime(val)
    return dt.isoformat() if dt else ""


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sample_users()
