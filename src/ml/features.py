import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import polars as pl

from src.db import get_db_connection

SQL_PATH = Path(__file__).resolve().parents[2] / "src" / "etl" / "transform" / "user_features.sql"
DEFAULT_SEMANTIC_FEATURES_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "processed"
    / "tweet_semantic_repetition_features.parquet"
)


def ensure_user_features(force: bool = False) -> None:
    """Creates or refreshes the `user_features` materialized view if needed.

    If the matview exists and `force` is False, the function returns immediately (idempotence).
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT matviewname
                FROM pg_matviews
                WHERE schemaname = 'public' AND matviewname = 'user_features'
                """
            )
            exists = cur.fetchone() is not None

            if exists and not force:
                return

            sql = SQL_PATH.read_text(encoding="utf-8")
            cur.execute(sql)
            conn.commit()


def get_user_features(
    user_ids: list[int] | None = None,
    include_semantic: bool = False,
    semantic_features_path: str | Path = DEFAULT_SEMANTIC_FEATURES_PATH,
) -> pl.DataFrame:
    """Returns the feature matrix from the `user_features` materialized view.

    Args:
        user_ids: Optional list of user IDs to filter by.
        include_semantic: If True, join cached tweet semantic repetition features.
        semantic_features_path: Path to the sidecar Parquet semantic feature table.

    Returns:
        Polars DataFrame containing one row per user and all
        the pre-calculated features from the matview.
    """
    ensure_user_features()

    query = "SELECT * FROM user_features"
    params = None
    if user_ids:
        query += " WHERE user_id = ANY(%s)"
        params = (user_ids,)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    features = pl.DataFrame(rows, schema=cols, orient="row")
    if include_semantic:
        features = join_semantic_repetition_features(features, semantic_features_path)

    return features


def load_semantic_repetition_features(
    semantic_features_path: str | Path = DEFAULT_SEMANTIC_FEATURES_PATH,
) -> pl.DataFrame:
    path = Path(semantic_features_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Semantic feature file not found: {path}. Build it with "
            "`uv run python -m src.etl.features.tweet_semantic_repetition "
            "--output data/processed/tweet_semantic_repetition_features.parquet`."
        )

    return pl.read_parquet(path).with_columns(pl.col("user_id").cast(pl.Int64))


def join_semantic_repetition_features(
    features: pl.DataFrame,
    semantic_features_path: str | Path = DEFAULT_SEMANTIC_FEATURES_PATH,
) -> pl.DataFrame:
    semantic_features = load_semantic_repetition_features(semantic_features_path)
    return features.with_columns(pl.col("user_id").cast(pl.Int64)).join(
        semantic_features,
        on="user_id",
        how="left",
    )


def get_labeled_features(
    csv_path: str,
    include_semantic: bool = False,
    semantic_features_path: str | Path = DEFAULT_SEMANTIC_FEATURES_PATH,
) -> pl.DataFrame:
    """Loads labels from the CSV and joins with features from the matview.

    Args:
        csv_path: Path to the labeled CSV file (e.g.
            `data/processed/labeling_sample.csv`).
        include_semantic: If True, join cached tweet semantic repetition features.
        semantic_features_path: Path to the sidecar Parquet semantic feature table.

    Returns:
        Polars DataFrame containing features and the `label` column (Int8).
    """
    labels = pl.read_csv(csv_path, separator=";")
    labels = (
        labels.select(["user_id", "label"])
        .with_columns(pl.col("label").cast(pl.Utf8))
        .filter(pl.col("label").is_in(["0", "1", "0,0"]))
        .with_columns(pl.col("label").replace({"0,0": "0"}).cast(pl.Int8))
    )

    user_ids = labels["user_id"].to_list()
    features = get_user_features(
        user_ids,
        include_semantic=include_semantic,
        semantic_features_path=semantic_features_path,
    )

    return features.join(labels, on="user_id", how="inner")


if __name__ == "__main__":
    # test rapide
    # print("Nombre total d'utilisateurs avec features :", len(get_user_features()))
    # ensure_user_features(force=True)
    print("Done")
