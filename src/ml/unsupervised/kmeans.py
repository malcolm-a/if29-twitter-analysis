import sys
from pathlib import Path

project_root = Path.cwd().resolve()
while project_root != project_root.parent and not (project_root / "pyproject.toml").exists():
    project_root = project_root.parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import numpy as np
import polars as pl
from sklearn.cluster import KMeans
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from src.ml.features import get_user_features

DROP_COLS: list[str] = [
    "user_id",
    "screen_name",
    "name",
    "created_at",
    "last_tweet_at",
    "first_seen_at",
    "last_seen_at",
    "profile_url",
    "description",
]


def _feature_cols(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in DROP_COLS]


def _build_pipeline(k: int, random_state: int = 42) -> Pipeline:
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  RobustScaler()),
        ("kmeans",  KMeans(n_clusters=k, random_state=random_state, n_init="auto")),
    ])


def fit_kmeans(
    df: pl.DataFrame,
    k: int,
    random_state: int = 42,
) -> tuple[Pipeline, pl.DataFrame]:
    """Fit K-Means on `df` and return the fitted pipeline and the dataframe with a `cluster` column.

    Descriptive columns (DROP_COLS) are excluded automatically.

    Args:
        df: Feature matrix — must contain `user_id`.
        k: Number of clusters.
        random_state: Seed for reproducibility.

    Returns:
        (pipeline, df_with_clusters)
    """
    X = df.select(_feature_cols(df)).to_numpy().astype(float)
    pipeline = _build_pipeline(k, random_state)
    labels = pipeline.fit_predict(X)
    return pipeline, df.with_columns(pl.Series("cluster", labels.astype(int)))


def elbow_curve(
    df: pl.DataFrame,
    k_range: range = range(2, 11),
    random_state: int = 42,
) -> pl.DataFrame:
    """Compute inertia for each k to help choose the optimal number of clusters.

    Returns:
        Polars DataFrame with columns `k` and `inertia`.
    """
    X = df.select(_feature_cols(df)).to_numpy().astype(float)
    prep = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  RobustScaler()),
    ])
    X_prep = prep.fit_transform(X)

    records = []
    for k in k_range:
        km = KMeans(n_clusters=k, random_state=random_state, n_init="auto")
        km.fit(X_prep)
        records.append({"k": k, "inertia": float(km.inertia_)})

    return pl.DataFrame(records)


def cluster_summary(df_with_clusters: pl.DataFrame) -> pl.DataFrame:
    """Return per-cluster mean for each feature plus the cluster size."""
    cols = _feature_cols(df_with_clusters)
    return (
        df_with_clusters
        .group_by("cluster")
        .agg(
            [pl.len().alias("n_users")]
            + [pl.col(c).mean().alias(c) for c in cols]
        )
        .sort("cluster")
    )


if __name__ == "__main__":
    print("Loading features...")
    df = get_user_features()
    print(f"  {len(df)} users, {len(df.columns)} columns")

    print("\nElbow curve (k=2..10):")
    elbow = elbow_curve(df)
    print(elbow)

    K = 4
    print(f"\nFitting K-Means with k={K}...")
    pipeline, df_clustered = fit_kmeans(df, k=K)

    print("\nCluster sizes:")
    print(df_clustered.group_by("cluster").agg(pl.len().alias("n")).sort("cluster"))

    print("\nCluster summary (feature means):")
    print(cluster_summary(df_clustered))
