"""
Extracts unique Twitter users from the `tweets` table, keeping only
the *latest* snapshot for each user (i.e. the user object from the
most recent tweet we have).

Strategy:
    For users who appear in multiple tweets, their follower count, bio,
    screen_name etc. may differ across snapshots.  We pick the version
    attached to the most recent tweet (by ``created_at``).  This gives us
    the freshest picture of the user *at collection time*.

Returns:
    A list of dicts ready to be upserted into the `users` table.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


def extract_users(
    conn: psycopg2.extensions.connection,
    *,
    batch_size: int = 5000,
) -> list[dict[str, Any]]:
    """Walk the `tweets` table, extract distinct users and keep the
    latest snapshot for each.

    Queries rely on the expression index
    ``idx_tweets_user_id`` (see ``init.sql``) for performance.
    A server-side cursor keeps memory usage constant regardless of
    the number of rows.

    Args:
        conn: Open psycopg2 connection to the PostgreSQL database.
        batch_size: How many rows to fetch per server-side cursor round-trip.

    Returns:
        One dict per user, with keys matching the `users` table columns.
    """
    users: dict[int, dict] = {}

    # DISTINCT ON keeps the first row for each user_id after sorting.
    # Sorting by user_id, created_at DESC gives us the latest tweet.
    query = """
        SELECT DISTINCT ON ((raw_data->'user'->>'id')::BIGINT)
            (raw_data->'user'->>'id')::BIGINT          AS user_id,
            raw_data->'user'                            AS raw_user,
            created_at                                  AS tweet_created_at
        FROM tweets
        WHERE raw_data->'user'->>'id' IS NOT NULL
        ORDER BY (raw_data->'user'->>'id')::BIGINT, created_at DESC
    """

    with conn.cursor(name="extract_users_cursor", cursor_factory=RealDictCursor) as cur:
        cur.itersize = batch_size
        cur.execute(query)

        for row in cur:
            uid = row["user_id"]
            raw_user = row["raw_user"]
            tca = row["tweet_created_at"]

            if uid not in users:
                # First (and latest, thanks to ORDER BY) encounter
                users[uid] = _parse_user(uid, raw_user, tca)
            # else: this is an older snapshot → skip

    logger.info("Extracted %d unique users from tweets table.", len(users))

    # Second pass: compute tweet_count, first_seen, last_seen per user
    _enrich_user_stats(conn, users)

    return list(users.values())


# ============== Helpers ==============


def _parse_user(uid: int, raw_user: dict, tweet_created_at: Any) -> dict:
    """Convert a raw Twitter user object into our clean schema.

    Args:
        uid: Twitter user ID.
        raw_user: The ``user`` sub-object from a tweet's JSON.
        tweet_created_at: Datetime of the tweet this snapshot came from.

    Returns:
        A dict with keys matching the `users` table columns.
    """
    created_at = raw_user.get("created_at")
    if isinstance(created_at, str):
        created_at = _parse_twitter_date(created_at)

    return {
        "user_id": uid,
        "screen_name": raw_user.get("screen_name"),
        "name": raw_user.get("name"),
        "description": raw_user.get("description"),
        "location": raw_user.get("location"),
        "url": raw_user.get("url"),
        "followers_count": raw_user.get("followers_count"),
        "friends_count": raw_user.get("friends_count"),
        "listed_count": raw_user.get("listed_count"),
        "favourites_count": raw_user.get("favourites_count"),
        "statuses_count": raw_user.get("statuses_count"),
        "verified": raw_user.get("verified"),
        "created_at": created_at,
        "profile_image_url": raw_user.get("profile_image_url"),
        "default_profile": raw_user.get("default_profile"),
        "default_profile_image": raw_user.get("default_profile_image"),
        "raw_user": raw_user,
        "tweet_count": 0,
        "last_tweet_at": tweet_created_at,
        "first_seen_at": tweet_created_at,
        "last_seen_at": tweet_created_at,
    }


def _enrich_user_stats(
    conn: psycopg2.extensions.connection,
    users: dict[int, dict],
) -> None:
    """Compute per-user aggregate stats from the `tweets` table.

    Populates ``tweet_count``, ``first_seen_at`` and ``last_seen_at``
    in-place on each user dict.

    Args:
        conn: Open psycopg2 database connection.
        users: Mapping of user_id → user dict (mutated in-place).
    """
    if not users:
        return

    query = """
        SELECT
            (raw_data->'user'->>'id')::BIGINT  AS user_id,
            COUNT(*)::int                        AS tweet_count,
            MIN(created_at)                      AS first_seen_at,
            MAX(created_at)                      AS last_seen_at
        FROM tweets
        WHERE (raw_data->'user'->>'id')::BIGINT = ANY(%s)
        GROUP BY (raw_data->'user'->>'id')::BIGINT
    """
    uids = list(users.keys())
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (uids,))
        for row in cur:
            uid = row["user_id"]
            if uid in users:
                users[uid]["tweet_count"] = row["tweet_count"]
                users[uid]["first_seen_at"] = row["first_seen_at"]
                users[uid]["last_seen_at"] = row["last_seen_at"]


def _parse_twitter_date(date_str: str):
    """Parse a Twitter-formatted date string into a datetime object.

    Args:
        date_str: A date string such as ``"Sun Jun 17 17:00:31 +0000 2018"``.

    Returns:
        The parsed datetime, or None if parsing fails.
    """
    from datetime import datetime

    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%a %b %d %H:%M:%S +0000 %Y")
    except Exception:
        return None
