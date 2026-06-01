import sys
from pathlib import Path

project_root = Path.cwd().resolve()
while project_root != project_root.parent and not (project_root / "pyproject.toml").exists():
    project_root = project_root.parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import numpy as np
import polars as pl
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from src.ml.features import get_labeled_features

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
    "label",
]


def _feature_cols(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in DROP_COLS]


def fit_pca(
    df: pl.DataFrame,
    n_components: int | float = 0.95,
    random_state: int = 42,
) -> tuple[Pipeline, np.ndarray]:
    """Fit a preprocessing + PCA pipeline and return the fitted pipeline and the transformed matrix.

    Args:
        df: Feature matrix — descriptive columns defined in DROP_COLS are excluded automatically.
        n_components: Number of components (int) or explained variance ratio (float in 0-1).
        random_state: Seed for reproducibility.

    Returns:
        (pipeline, X_transformed) where X_transformed has shape (n_users, n_components).
    """
    available = _feature_cols(df)
    X = df.select(available).to_numpy().astype(float)

    pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  RobustScaler()),
        ("pca",     PCA(n_components=n_components, random_state=random_state)),
    ])
    X_transformed = pipeline.fit_transform(X)
    return pipeline, X_transformed


def explained_variance(
    df: pl.DataFrame,
    max_components: int | None = None,
    random_state: int = 42,
) -> pl.DataFrame:
    """Compute per-component explained variance to help choose n_components.

    Args:
        df: Feature matrix — descriptive columns defined in DROP_COLS are excluded automatically.
        max_components: Upper bound on components to analyse (defaults to all features).

    Returns:
        Polars DataFrame with columns: `component`, `explained_variance_ratio`, `cumulative`.
    """
    available = _feature_cols(df)
    X = df.select(available).to_numpy().astype(float)

    prep = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  RobustScaler()),
    ])
    X_prep = prep.fit_transform(X)

    n = max_components or len(available)
    pca = PCA(n_components=n, random_state=random_state)
    pca.fit(X_prep)

    ratios = pca.explained_variance_ratio_
    return pl.DataFrame({
        "component":                range(1, len(ratios) + 1),
        "explained_variance_ratio": ratios.tolist(),
        "cumulative":               ratios.cumsum().tolist(),
    })


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PCA analysis on labeled users")
    parser.add_argument("--labels", default="data/processed/labeling_sample.csv")
    args = parser.parse_args()

    print(f"Loading labeled features from {args.labels}...")
    df = get_labeled_features(args.labels)
    print(f"  {len(df)} labeled users")

    print("\nExplained variance per component:")
    ev = explained_variance(df)
    print(ev)

    pipeline, X_t = fit_pca(df)
    n_kept = pipeline.named_steps["pca"].n_components_
    print(f"\nPCA (variance=0.95) kept {n_kept} components → shape {X_t.shape}")
