import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path.cwd().resolve()
while project_root != project_root.parent and not (project_root / "pyproject.toml").exists():
    project_root = project_root.parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import seaborn as sns
from sklearn.cluster import MiniBatchKMeans
from sklearn.decomposition import PCA
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

IMG_DIR = project_root / "src" / "eda" / "img_sample_analysis"

# MiniBatchKMeans instead of KMeans: with ~1.8M users, full Lloyd's iterations
# (run once per k in the elbow curve, plus the final fit) are far too slow/CPU-heavy
# for a local machine. batch_size/max_iter are raised above sklearn's defaults so
# each fit still samples enough of the dataset to converge to stable centroids.
_KMEANS_KWARGS = {"n_init": "auto", "batch_size": 4096, "max_iter": 200}


def _feature_cols(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in DROP_COLS]


def _build_pipeline(k: int, random_state: int = 42) -> Pipeline:
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  RobustScaler()),
        ("kmeans",  MiniBatchKMeans(n_clusters=k, random_state=random_state, **_KMEANS_KWARGS)),
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
        km = MiniBatchKMeans(n_clusters=k, random_state=random_state, **_KMEANS_KWARGS)
        km.fit(X_prep)
        records.append({"k": k, "inertia": float(km.inertia_)})

    return pl.DataFrame(records)


def cluster_summary(df_with_clusters: pl.DataFrame) -> pl.DataFrame:
    """Return per-cluster mean for each feature plus the cluster size."""
    cols = [c for c in _feature_cols(df_with_clusters) if c != "cluster"]
    return (
        df_with_clusters
        .group_by("cluster")
        .agg(
            [pl.len().alias("n_users")]
            + [pl.col(c).mean().alias(c) for c in cols]
        )
        .sort("cluster")
    )


def plot_elbow(elbow_df: pl.DataFrame, path: Path) -> None:
    """Save a line plot of inertia vs k (elbow method)."""
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(elbow_df["k"], elbow_df["inertia"], marker="o")
    ax.set_xlabel("Nombre de clusters (k)")
    ax.set_ylabel("Inertie")
    ax.set_title("Methode du coude - K-Means")
    ax.set_xticks(elbow_df["k"].to_list())
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_cluster_sizes(df_clustered: pl.DataFrame, path: Path) -> None:
    """Save a bar chart of the number of users per cluster."""
    sizes = df_clustered.group_by("cluster").agg(pl.len().alias("n")).sort("cluster")

    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar([f"Cluster {c}" for c in sizes["cluster"].to_list()], sizes["n"].to_list())
    ax.set_ylabel("Nombre d'utilisateurs")
    ax.set_title("Taille des clusters")
    for bar, n in zip(bars, sizes["n"].to_list()):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{n:,}", ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_cluster_profile(summary_df: pl.DataFrame, path: Path) -> None:
    """Save a heatmap of per-feature cluster means, normalized per feature for readability."""
    feature_cols = [c for c in summary_df.columns if c not in ("cluster", "n_users")]
    clusters = summary_df["cluster"].to_list()
    values = summary_df.select(feature_cols).to_numpy().astype(float).T  # features x clusters

    col_min = np.nanmin(values, axis=1, keepdims=True)
    col_max = np.nanmax(values, axis=1, keepdims=True)
    span = np.where(col_max - col_min == 0, 1, col_max - col_min)
    normalized = (values - col_min) / span

    fig, ax = plt.subplots(figsize=(1.5 * len(clusters) + 4, 0.4 * len(feature_cols) + 2))
    sns.heatmap(
        normalized,
        annot=values,
        fmt=".2f",
        cmap="viridis",
        xticklabels=[f"Cluster {c}" for c in clusters],
        yticklabels=feature_cols,
        cbar_kws={"label": "Valeur normalisee par feature (0-1)"},
        ax=ax,
    )
    ax.set_title("Profil moyen des clusters par feature")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_pca_scatter(
    df: pl.DataFrame,
    pipeline: Pipeline,
    labels: np.ndarray,
    path: Path,
    sample_size: int = 20_000,
    random_state: int = 42,
) -> None:
    """Save a 2D PCA scatter plot of the clusters (down-sampled for readability)."""
    X = df.select(_feature_cols(df)).to_numpy().astype(float)
    X_prep = pipeline[:-1].transform(X)

    pca = PCA(n_components=2, random_state=random_state)
    X_pca = pca.fit_transform(X_prep)

    rng = np.random.default_rng(random_state)
    idx = rng.choice(len(X_pca), size=min(sample_size, len(X_pca)), replace=False)

    fig, ax = plt.subplots(figsize=(8, 7))
    for cluster_id in sorted(np.unique(labels)):
        mask = labels[idx] == cluster_id
        ax.scatter(X_pca[idx][mask, 0], X_pca[idx][mask, 1], s=5, alpha=0.5, label=f"Cluster {cluster_id}")
    ax.set_xlabel(f"PC1 ({pca.explained_variance_ratio_[0]:.1%} var.)")
    ax.set_ylabel(f"PC2 ({pca.explained_variance_ratio_[1]:.1%} var.)")
    ax.set_title(f"Clusters K-Means projetes en 2D (PCA, echantillon de {len(idx):,} utilisateurs)")
    ax.legend(title="Cluster", markerscale=2)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    IMG_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading features...")
    df = get_user_features()
    print(f"  {len(df)} users, {len(df.columns)} columns")

    print("\nElbow curve (k=2..10):")
    elbow = elbow_curve(df)
    print(elbow)
    plot_elbow(elbow, IMG_DIR / "9-elbow_curve.png")

    K = 4
    print(f"\nFitting K-Means with k={K}...")
    pipeline, df_clustered = fit_kmeans(df, k=K)
    plot_cluster_sizes(df_clustered, IMG_DIR / "10-cluster_sizes.png")

    print("\nCluster sizes:")
    print(df_clustered.group_by("cluster").agg(pl.len().alias("n")).sort("cluster"))

    print("\nCluster summary (feature means):")
    summary = cluster_summary(df_clustered)
    print(summary)
    plot_cluster_profile(summary, IMG_DIR / "11-cluster_profiles.png")

    labels = df_clustered["cluster"].to_numpy()
    plot_pca_scatter(df, pipeline, labels, IMG_DIR / "12-pca_clusters.png")

    print(f"\nImages saved to {IMG_DIR}")
