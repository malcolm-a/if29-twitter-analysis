"""
Orchestrates the extraction of unique users from the `tweets` table
and upserts them into the `users` table.

Usage:
    python -m src.etl.load.load_users
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import sys

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from src.db import get_db_connection
from src.etl.transform.extract_users import extract_users

UPSERT_BATCH_SIZE = 1000
COMMIT_CHUNK_SIZE = 50_000

CHECKPOINT_FILE = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "..",
    "data",
    "processed",
    "extracted_users.pkl",
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def _run_init_sql(conn: psycopg2.extensions.connection) -> None:
    """Apply init.sql if not already applied (idempotent by design).

    Args:
        conn: An open psycopg2 database connection.
    """
    init_sql_path = os.path.join(os.path.dirname(__file__), "..", "extract", "init.sql")
    if not os.path.exists(init_sql_path):
        logger.warning("init.sql not found at %s — skipping.", init_sql_path)
        return

    logger.info("Applying init.sql ...")
    with open(init_sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("init.sql applied successfully.")


def load_users() -> None:
    """Full pipeline: init schema -> extract users -> upsert into users table."""
    logger.info("Connecting to database ...")

    try:
        with get_db_connection() as conn:
            _run_init_sql(conn)

        users = _extract_or_load_cache(conn)
        if not users:
            logger.warning("No users extracted - is the tweets table empty?")
            return

        _upsert_users(conn, users)
        logger.info("Upserted %d users into users table.", len(users))

        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            logger.info("Checkpoint file removed (upsert succeeded).")

    except Exception:
        logger.exception("load_users failed - changes rolled back.")
        raise


def _extract_or_load_cache(conn) -> list[dict]:
    """Extract users from tweets, or load from a pickle checkpoint.

    Args:
        conn: Open psycopg2 database connection.

    Returns:
        List of user dicts.
    """
    if os.path.exists(CHECKPOINT_FILE):
        logger.info(
            "Found checkpoint %s - loading users from cache ...", CHECKPOINT_FILE
        )
        with open(CHECKPOINT_FILE, "rb") as f:
            users = pickle.load(f)
        logger.info("Loaded %d users from checkpoint.", len(users))
        return users

    logger.info(
        "Extracting users from tweets table "
        "(this may take a while on large datasets) ..."
    )
    users = extract_users(conn)
    logger.info("Extracted %d unique users.", len(users))

    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "wb") as f:
        pickle.dump(users, f)
    logger.info("Checkpoint saved to %s.", CHECKPOINT_FILE)

    return users


def _upsert_users(conn, users: list[dict]) -> None:
    """Upsert a list of user dicts into the users table.

    Commits in chunks of COMMIT_CHUNK_SIZE to avoid a single
    giant transaction that would saturate the WAL.

    Args:
        conn: Open psycopg2 database connection.
        users: List of user dicts from extract_users().
    """
    columns = [
        "user_id",
        "screen_name",
        "name",
        "description",
        "location",
        "url",
        "followers_count",
        "friends_count",
        "listed_count",
        "favourites_count",
        "statuses_count",
        "verified",
        "created_at",
        "profile_image_url",
        "default_profile",
        "default_profile_image",
        "raw_user",
        "tweet_count",
        "last_tweet_at",
        "first_seen_at",
        "last_seen_at",
    ]
    update_set = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in columns if col != "user_id"
    )

    upsert_sql = f"""
        INSERT INTO users ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (user_id) DO UPDATE SET {update_set}
    """

    total = len(users)

    for offset in range(0, total, COMMIT_CHUNK_SIZE):
        chunk = users[offset : offset + COMMIT_CHUNK_SIZE]
        rows = []
        for u in chunk:
            row = tuple(
                json.dumps(u.get(c)) if c == "raw_user" else u.get(c) for c in columns
            )
            rows.append(row)

        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows, page_size=UPSERT_BATCH_SIZE)
        conn.commit()

        done = min(offset + COMMIT_CHUNK_SIZE, total)
        logger.info("Progress: %d / %d users upserted.", done, total)


if __name__ == "__main__":
    load_users()
