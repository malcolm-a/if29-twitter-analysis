import sys
import os
import polars as pl
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from src.db import get_db_connection

REFERENCE_DATE = datetime(2018, 6, 17)

def get_tweets(limit: int | None = None) -> pl.DataFrame:
    """Return the `tweets` table as a Polars DataFrame."""
    query = "SELECT * FROM tweets"   
    if limit:
        query += f" limit {limit}"

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

    return pl.DataFrame(rows, schema=columns, orient="row")

def add_tweet_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([
        pl.col("tweet_text").str.count_matches(r"https?://\S+").alias("url_count"),
        pl.col("tweet_text").str.count_matches(r"@\w+").alias("mention_count"),
        pl.col("tweet_text").str.count_matches(r"#\w+").alias("hashtag_count"),
        pl.col("tweet_text").str.len_chars().alias("tweet_length"),
        pl.col("tweet_text").str.starts_with("RT ").cast(pl.Int8).alias("is_retweet"),

        pl.col("created_at").dt.hour().alias("tweet_hour"),

        pl.col("raw_data").struct.field("reply_count").cast(pl.Int32).alias("reply_count"),
        pl.col("raw_data").struct.field("retweet_count").cast(pl.Int32).alias("retweet_count"),
        pl.col("raw_data").struct.field("favorite_count").cast(pl.Int32).alias("favorite_count"),
        pl.col("raw_data").struct.field("quote_count").cast(pl.Int32).alias("quote_count"),
        pl.col("raw_data").struct.field("is_quote_status").cast(pl.Boolean).alias("is_quote"),
        pl.col("raw_data").struct.field("in_reply_to_user_id").is_not_null().cast(pl.Int8).alias("is_reply"),
        pl.col("raw_data").struct.field("source").alias("source"),
        pl.col("raw_data").struct.field("lang").alias("lang"),
    ])

if __name__ == "__main__":
    df = get_tweets(10)
    df = add_tweet_features(df)
    print(df)