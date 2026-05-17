import os
import sys
from datetime import datetime

import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from src.db import get_db_connection

REFERENCE_DATE = datetime(2018, 6, 17)


def get_users(limit: int | None = None) -> pl.DataFrame:
    """Return the `users` table as a Polars DataFrame."""
    query = "SELECT * FROM users"
    if limit:
        query += f" limit {limit}"

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

    return pl.DataFrame(rows, schema=columns, orient="row")


# Ratios & interactions
def add_ratio_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Returns: dataframe with new columns :
        - followers_friends_ratio: ratio between the number of followers and friends
        - favourites_per_status: average engagement per status
        - listed_per_follower: popularity in list
    """
    return df.with_columns(
        [
            (pl.col("followers_count") / (pl.col("friends_count") + 1)).alias(
                "followers_friends_ratio"
            ),
            (pl.col("favourites_count") / (pl.col("statuses_count") + 1)).alias(
                "favourites_per_status"
            ),
            (pl.col("listed_count") / (pl.col("followers_count") + 1)).alias("listed_per_follower"),
        ]
    )


# Temporal features
def add_temporal_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Returns:
    """
    return df.with_columns(
        [
            (pl.lit(REFERENCE_DATE) - pl.col("created_at"))
            .dt.total_days()
            .alias("account_age_days"),
            (pl.lit(REFERENCE_DATE) - pl.col("last_tweet_at"))
            .dt.total_days()
            .alias("days_since_last_tweet"),
            (pl.col("last_seen_at") - pl.col("first_seen_at"))
            .dt.total_days()
            .alias("observation_span_days"),
        ]
    ).with_columns(
        [
            (pl.col("tweet_count") / (pl.col("account_age_days") + 1)).alias("tweet_frequency"),
        ]
    )


# Textual features
def add_textual_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.col("description").fill_null("").str.len_chars().alias("description_length"),
            pl.col("description").is_not_null().cast(pl.Int8).alias("has_description"),
            pl.col("url").is_not_null().cast(pl.Int8).alias("has_url"),
            pl.col("location").is_not_null().cast(pl.Int8).alias("has_location"),
            pl.col("screen_name").str.len_chars().alias("screen_name_length"),
        ]
    )


# Indicators
def add_boolean_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            (
                pl.col("has_description")
                + pl.col("has_url")
                + pl.col("has_location")
                + pl.col("verified").cast(pl.Int8)
                + pl.col("default_profile_image").not_().cast(pl.Int8)
            ).alias("profile_completeness"),
            (pl.col("account_age_days") < 30).cast(pl.Int8).alias("is_new_account"),
            (pl.col("days_since_last_tweet") > 90).cast(pl.Int8).alias("is_inactive"),
        ]
    )


if __name__ == "__main__":
    df = get_users(1000)
    df = add_ratio_features(df)
    df = add_temporal_features(df)
    df = add_textual_features(df)
    df = add_boolean_features(df)
    print(df.head(10))
