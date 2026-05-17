import sys
import os
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

TWITTER_API_MAX_ACTIONS = 350  # fmax from SPOT paper
AVG_MENTION_CHARS = 11.4       # C(@) from SPOT paper
AVG_HASHTAG_CHARS = 11.6       # C(#) from SPOT paper
TWEET_MAX_CHARS = 140          # tweet max length at the time of the paper


def add_spot_tweet_aggregates(df_tweets: pl.DataFrame) -> pl.DataFrame:
    """
    Agrège les features tweets par user_id.
    A appeler sur le DataFrame issu de tweet_features.py
    """
    return df_tweets.group_by("user_id").agg([
        pl.col("url_count").mean().alias("urls_per_tweet"),
        pl.col("hashtag_count").mean().alias("hashtags_per_tweet"),
        pl.col("mention_count").mean().alias("references_per_tweet"),
        pl.col("is_retweet").mean().alias("retweet_rate"),
        pl.col("is_reply").mean().alias("reply_rate"),
        pl.col("is_automated_source").mean().alias("automated_source_rate"),
        pl.col("tweet_hour").std().alias("tweet_hour_std"),
        pl.col("tweet_length").mean().alias("avg_tweet_length"),
        pl.col("retweet_count").mean().alias("avg_retweet_count"),
        pl.col("favorite_count").mean().alias("avg_favorite_count"),
    ])


def add_spot_scores(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calcule les 3 dimensions du score SPOT (section III du papier).
    Requiert un DataFrame jointure entre user_features et tweet_aggregates.

    - spot_aggressiveness : vitesse d'action du profil
    - spot_visibility     : efficacité de diffusion des tweets
    - spot_danger         : à compléter quand les URLs seront classifiées
    """
    return df.with_columns([
        (
            (pl.col("tweet_frequency_per_hour") + pl.col("friends_per_hour"))
            / TWITTER_API_MAX_ACTIONS
        ).alias("spot_aggressiveness"),
        (
            (pl.col("references_per_tweet") * AVG_MENTION_CHARS +
             pl.col("hashtags_per_tweet") * AVG_HASHTAG_CHARS)
            / TWEET_MAX_CHARS
        ).alias("spot_visibility"),
        pl.lit(None).cast(pl.Float32).alias("spot_danger"),
    ])


def build_spot_dataframe(
    df_users: pl.DataFrame,
    df_tweets: pl.DataFrame,
) -> pl.DataFrame:
    """
    Pipeline complet : joint users et tweets, calcule tous les scores SPOT.
    
    Args:
        df_users  : DataFrame issu de user_features.py (toutes les features ajoutées)
        df_tweets : DataFrame issu de tweet_features.py (toutes les features ajoutées)
    
    Returns:
        DataFrame avec les scores SPOT par user
    """
    df_tweet_agg = add_spot_tweet_aggregates(df_tweets)

    return (
        df_users
        .join(df_tweet_agg, on="user_id", how="left")
        .pipe(add_spot_scores)
    )


if __name__ == "__main__":
    from user_features import (
        get_users,
        add_ratio_features,
        add_temporal_features,
        add_textual_features,
        add_boolean_features,
    )

    # Pipeline users
    df_users = get_users(1000)
    df_users = add_ratio_features(df_users)
    df_users = add_temporal_features(df_users)
    df_users = add_textual_features(df_users)
    df_users = add_boolean_features(df_users)

    from tweet_features import get_tweets, add_tweet_features, add_source_features
    df_tweets = get_tweets()
    df_tweets = add_tweet_features(df_tweets)
    df_tweets = add_source_features(df_tweets)

    df_spot = build_spot_dataframe(df_users, df_tweets)
    print(df_spot.select([
        "user_id", "spot_aggressiveness", "spot_visibility", "spot_danger"
    ]).head(10))