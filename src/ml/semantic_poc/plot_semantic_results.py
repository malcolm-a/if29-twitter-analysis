from __future__ import annotations

import argparse
import os
import sys
import textwrap
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import polars as pl
import seaborn as sns
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from src.ml.features import get_labeled_features
from src.ml.semantic_poc.compare_semantic_features import (
    BEST_RF_PARAMS,
    DEFAULT_CACHE_DIR,
    prepare_xy,
)

DEFAULT_LABELS_PATH = Path("data/processed/labeling_sample.csv")
DEFAULT_SEMANTIC_PATH = Path(
    "data/processed/semantic_poc/tweet_semantic_repetition_features_labeled.parquet"
)
DEFAULT_OUTPUT_DIR = Path("docs/report/img/semantic_poc")

FONT_FAMILY = ["Aptos", "Inter", "Segoe UI", "DejaVu Sans", "Arial", "sans-serif"]
MONO_FONT_FAMILY = ["SF Mono", "Menlo", "Consolas", "DejaVu Sans Mono", "monospace"]

TOKENS = {
    "surface": "#FCFCFD",
    "panel": "#FFFFFF",
    "ink": "#1F2430",
    "muted": "#6F768A",
    "grid": "#E6E8F0",
    "axis": "#D7DBE7",
}

COLOR_FAMILIES = {
    "blue": {
        "xlight": "#EAF1FE",
        "light": "#CEDFFE",
        "base": "#A3BEFA",
        "mid": "#5477C4",
        "dark": "#2E4780",
    },
    "gold": {
        "xlight": "#FFF4C2",
        "light": "#FFEA8F",
        "base": "#FFE15B",
        "mid": "#B8A037",
        "dark": "#736422",
    },
    "orange": {
        "xlight": "#FFEDDE",
        "light": "#FFBDA1",
        "base": "#F0986E",
        "mid": "#CC6F47",
        "dark": "#804126",
    },
    "pink": {
        "xlight": "#FCDAD6",
        "light": "#F5BACC",
        "base": "#F390CA",
        "mid": "#BD569B",
        "dark": "#8A3A6F",
    },
}

LABEL_NAMES = {0: "Human", 1: "Bot"}
LABEL_PALETTE = {"Human": COLOR_FAMILIES["blue"]["mid"], "Bot": COLOR_FAMILIES["orange"]["mid"]}
RUN_PALETTE = {
    "Baseline": COLOR_FAMILIES["blue"]["base"],
    "Semantic": COLOR_FAMILIES["gold"]["base"],
}

SEMANTIC_CORRELATION_COLS = [
    "semantic_similarity_mean",
    "semantic_similarity_p90",
    "normalized_duplicate_rate",
    "unique_normalized_tweet_ratio",
]

BEHAVIOR_CORRELATION_COLS = [
    "tweet_frequency",
    "retweet_rate",
    "urls_per_tweet",
    "hashtags_per_tweet",
    "mentions_per_tweet",
    "bot_source_ratio",
    "tweet_count",
    "followers_friends_ratio",
]


def use_chart_theme() -> None:
    sns.set_theme(
        style="whitegrid",
        rc={
            "figure.facecolor": TOKENS["surface"],
            "figure.edgecolor": "none",
            "savefig.facecolor": TOKENS["surface"],
            "savefig.edgecolor": "none",
            "axes.facecolor": TOKENS["panel"],
            "axes.edgecolor": TOKENS["axis"],
            "axes.labelcolor": TOKENS["ink"],
            "axes.grid": True,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "grid.color": TOKENS["grid"],
            "grid.linewidth": 0.8,
            "font.family": "sans-serif",
            "font.sans-serif": FONT_FAMILY,
            "font.monospace": MONO_FONT_FAMILY,
            "patch.linewidth": 1.0,
        },
    )
    plt.rcParams["figure.dpi"] = 140


def add_chart_header(
    fig,
    ax,
    title: str,
    subtitle: str,
    *,
    title_width: int = 82,
    subtitle_width: int = 118,
) -> None:
    title = textwrap.fill(title.strip(), width=title_width, break_long_words=False)
    subtitle = textwrap.fill(subtitle.strip(), width=subtitle_width, break_long_words=False)
    title_lines = title.count("\n") + 1
    subtitle_lines = subtitle.count("\n") + 1

    ax.set_title("")
    fig.subplots_adjust(
        top=max(0.62, 0.86 - 0.045 * (title_lines - 1) - 0.032 * (subtitle_lines - 1))
    )

    left = ax.get_position().x0
    fig.text(
        left,
        0.985,
        title,
        ha="left",
        va="top",
        fontsize=13,
        fontweight="semibold",
        color=TOKENS["ink"],
        linespacing=1.08,
    )
    fig.text(
        left,
        0.93 - 0.045 * (title_lines - 1),
        subtitle,
        ha="left",
        va="top",
        fontsize=9,
        color=TOKENS["muted"],
        linespacing=1.18,
    )
    sns.despine(fig=fig)


def load_model_frames(
    labels_csv: Path,
    semantic_features: Path,
    features_snapshot: Path | None,
    feature_cache_dir: Path | None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if features_snapshot is not None:
        baseline = load_features_snapshot(features_snapshot).drop_nans("label")
        semantic = baseline.join(
            pl.read_parquet(semantic_features).with_columns(pl.col("user_id").cast(pl.Int64)),
            on="user_id",
            how="left",
        )
    else:
        try:
            baseline = get_labeled_features(str(labels_csv)).drop_nans("label")
            semantic = get_labeled_features(
                str(labels_csv),
                include_semantic=True,
                semantic_features_path=semantic_features,
            ).drop_nans("label")
        except Exception:
            if feature_cache_dir is None:
                raise
            baseline_path = feature_cache_dir / "baseline_features.parquet"
            semantic_path = feature_cache_dir / "semantic_features.parquet"
            if not baseline_path.exists() or not semantic_path.exists():
                raise
            print(
                "Live DB load failed; using cached live feature matrices from "
                f"{feature_cache_dir}."
            )
            baseline = pl.read_parquet(baseline_path).drop_nans("label")
            semantic = pl.read_parquet(semantic_path).drop_nans("label")

    if baseline.height != semantic.height:
        raise ValueError(
            f"Row count changed after semantic join: baseline={baseline.height}, "
            f"semantic={semantic.height}"
        )
    return baseline, semantic


def load_features_snapshot(features_snapshot: Path) -> pl.DataFrame:
    if not features_snapshot.exists():
        raise FileNotFoundError(f"Feature snapshot not found: {features_snapshot}")

    df = pl.read_csv(features_snapshot).with_columns(pl.col("user_id").cast(pl.Int64))
    if "kmeans_cluster" in df.columns:
        df = df.drop("kmeans_cluster")
    return df


def train_run(df: pl.DataFrame) -> dict[str, object]:
    x, y, feature_cols = prepare_xy(df)
    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=0.2,
        random_state=1,
        stratify=y.to_numpy(),
    )
    model = RandomForestClassifier(random_state=1, **BEST_RF_PARAMS)
    model.fit(x_train.to_numpy(), y_train.to_numpy())
    y_pred = model.predict(x_test.to_numpy())

    return {
        "model": model,
        "feature_cols": feature_cols,
        "x": x,
        "y": y,
        "x_test": x_test,
        "y_test": y_test,
        "y_pred": y_pred,
        "metrics": {
            "Accuracy": accuracy_score(y_test.to_numpy(), y_pred),
            "Precision": precision_score(y_test.to_numpy(), y_pred, zero_division=0),
            "Recall": recall_score(y_test.to_numpy(), y_pred, zero_division=0),
            "F1": f1_score(y_test.to_numpy(), y_pred, zero_division=0),
        },
    }


def save_figure(fig, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_metric_comparison(
    baseline_run: dict[str, object],
    semantic_run: dict[str, object],
    output_dir: Path,
) -> Path:
    rows = []
    for run_name, run in [("Baseline", baseline_run), ("Semantic", semantic_run)]:
        for metric, value in run["metrics"].items():
            rows.append({"run": run_name, "metric": metric, "value": value})
    plot_df = pl.DataFrame(rows).to_pandas()

    fig, ax = plt.subplots(figsize=(8.8, 5.2))
    sns.barplot(
        data=plot_df,
        x="metric",
        y="value",
        hue="run",
        palette=RUN_PALETTE,
        edgecolor=TOKENS["ink"],
        linewidth=0.8,
        ax=ax,
    )
    ax.set_ylim(0, 1.08)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    ax.set_xlabel("")
    ax.set_ylabel("Score")
    ax.legend(loc="upper left", frameon=False, ncol=2)

    for container in ax.containers:
        ax.bar_label(container, fmt="%.3f", padding=3, fontsize=8, color=TOKENS["ink"])

    add_chart_header(
        fig,
        ax,
        "Semantic features improve bot recall on the labeled split",
        "Random Forest with fixed best parameters; 420 labeled users, 80/20 stratified split, random_state=1.",
    )
    output_path = output_dir / "semantic_rf_metrics.png"
    save_figure(fig, output_path)
    return output_path


def plot_confusion_matrices(
    baseline_run: dict[str, object],
    semantic_run: dict[str, object],
    output_dir: Path,
) -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(9.8, 4.8), sharey=True)
    cmap = sns.blend_palette(
        [TOKENS["panel"], COLOR_FAMILIES["blue"]["light"], COLOR_FAMILIES["blue"]["mid"]],
        as_cmap=True,
    )

    for ax, title, run in [
        (axes[0], "Baseline", baseline_run),
        (axes[1], "Semantic features", semantic_run),
    ]:
        matrix = confusion_matrix(run["y_test"].to_numpy(), run["y_pred"], labels=[0, 1])
        sns.heatmap(
            matrix,
            annot=True,
            fmt="d",
            cmap=cmap,
            cbar=False,
            linewidths=1.0,
            linecolor=TOKENS["panel"],
            xticklabels=["Human", "Bot"],
            yticklabels=["Human", "Bot"],
            ax=ax,
        )
        ax.set_title(title, fontsize=11, color=TOKENS["ink"], pad=10)
        ax.set_xlabel("Predicted label")
        ax.set_ylabel("True label" if ax is axes[0] else "")

    add_chart_header(
        fig,
        axes[0],
        "Confusion matrices show the recall gain",
        "Semantic-enhanced Random Forest removes the remaining false negative on this test split.",
    )
    fig.subplots_adjust(top=0.76, wspace=0.20)
    axes[0].set_title("Baseline", fontsize=11, color=TOKENS["ink"], pad=10)
    axes[1].set_title("Semantic features", fontsize=11, color=TOKENS["ink"], pad=10)
    output_path = output_dir / "semantic_confusion_matrices.png"
    save_figure(fig, output_path)
    return output_path


def plot_semantic_feature_distributions(df: pl.DataFrame, output_dir: Path) -> Path:
    selected = [
        ("normalized_duplicate_rate", "Normalized duplicate rate"),
        ("semantic_similarity_mean", "Mean semantic similarity"),
        ("semantic_similarity_p90", "P90 semantic similarity"),
    ]
    plot_df = (
        df.select(["label", *[column for column, _label in selected]])
        .with_columns(
            pl.when(pl.col("label") == 1)
            .then(pl.lit("Bot"))
            .otherwise(pl.lit("Human"))
            .alias("profile_type")
        )
        .unpivot(
            index=["profile_type"],
            on=[column for column, _label in selected],
            variable_name="feature",
            value_name="value",
        )
        .drop_nulls("value")
        .with_columns(
            pl.col("feature")
            .replace({column: label for column, label in selected})
            .alias("feature")
        )
        .to_pandas()
    )

    fig, axes = plt.subplots(1, 3, figsize=(12.4, 4.8), sharex=False)
    for ax, (_column, label) in zip(axes, selected):
        part = plot_df[plot_df["feature"] == label]
        sns.boxplot(
            data=part,
            x="profile_type",
            y="value",
            hue="profile_type",
            order=["Human", "Bot"],
            palette=LABEL_PALETTE,
            legend=False,
            linewidth=1.0,
            ax=ax,
        )
        sns.stripplot(
            data=part,
            x="profile_type",
            y="value",
            order=["Human", "Bot"],
            color=TOKENS["ink"],
            alpha=0.22,
            size=2.4,
            jitter=0.22,
            ax=ax,
        )
        ax.set_title(label, fontsize=10, color=TOKENS["ink"])
        ax.set_xlabel("")
        ax.set_ylabel("Feature value" if ax is axes[0] else "")

    add_chart_header(
        fig,
        axes[0],
        "Raw semantic repetition features are weak as standalone separators",
        "Distributions by manual label; useful model lift likely comes from interactions with existing profile and activity variables.",
    )
    output_path = output_dir / "semantic_feature_distributions.png"
    save_figure(fig, output_path)
    return output_path


def plot_pca_projection(semantic_df: pl.DataFrame, semantic_run: dict[str, object], output_dir: Path) -> Path:
    x = semantic_run["x"]
    y = semantic_run["y"].to_numpy()
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x.to_numpy())
    pca = PCA(n_components=2)
    coordinates = pca.fit_transform(x_scaled)

    plot_df = pl.DataFrame(
        {
            "pc1": coordinates[:, 0],
            "pc2": coordinates[:, 1],
            "label": y,
            "profile_type": [LABEL_NAMES[int(value)] for value in y],
            "tweet_count": semantic_df["semantic_tweet_count"].fill_null(0).to_numpy(),
            "similarity": semantic_df["semantic_similarity_mean"].fill_null(0).to_numpy(),
        }
    ).to_pandas()

    fig, axes = plt.subplots(1, 2, figsize=(12.6, 5.4), sharex=True, sharey=True)
    sns.scatterplot(
        data=plot_df,
        x="pc1",
        y="pc2",
        hue="profile_type",
        hue_order=["Human", "Bot"],
        palette=LABEL_PALETTE,
        edgecolor=TOKENS["panel"],
        linewidth=0.5,
        alpha=0.78,
        s=44,
        ax=axes[0],
    )
    axes[0].set_title("Manual labels", fontsize=10, color=TOKENS["ink"])
    axes[0].legend(loc="upper left", frameon=False, ncol=2)

    scatter = axes[1].scatter(
        plot_df["pc1"],
        plot_df["pc2"],
        c=plot_df["similarity"],
        s=np.clip(22 + plot_df["tweet_count"] * 5, 24, 120),
        cmap=sns.blend_palette(
            [
                COLOR_FAMILIES["gold"]["xlight"],
                COLOR_FAMILIES["gold"]["base"],
                COLOR_FAMILIES["orange"]["mid"],
            ],
            as_cmap=True,
        ),
        edgecolors=TOKENS["panel"],
        linewidths=0.5,
        alpha=0.78,
    )
    axes[1].set_title("Semantic similarity intensity", fontsize=10, color=TOKENS["ink"])
    colorbar = fig.colorbar(scatter, ax=axes[1], fraction=0.046, pad=0.04)
    colorbar.set_label("Mean semantic similarity")
    colorbar.outline.set_visible(False)

    for ax in axes:
        ax.set_xlabel(f"PC1 ({pca.explained_variance_ratio_[0] * 100:.1f}%)")
        ax.set_ylabel(f"PC2 ({pca.explained_variance_ratio_[1] * 100:.1f}%)")

    add_chart_header(
        fig,
        axes[0],
        "ACP projection with semantic features added",
        "StandardScaler plus PCA on the 420 labeled profiles; marker size on the right reflects embedded tweet count.",
    )
    fig.subplots_adjust(top=0.78, wspace=0.18)
    output_path = output_dir / "semantic_pca_projection.png"
    save_figure(fig, output_path)
    return output_path


def plot_semantic_behavior_correlations(semantic_df: pl.DataFrame, output_dir: Path) -> Path:
    semantic_cols = [column for column in SEMANTIC_CORRELATION_COLS if column in semantic_df.columns]
    behavior_cols = [column for column in BEHAVIOR_CORRELATION_COLS if column in semantic_df.columns]
    if not semantic_cols or not behavior_cols:
        raise ValueError("No overlapping semantic/behavior columns available for correlation plot.")

    corr_df = (
        semantic_df.select(semantic_cols + behavior_cols)
        .with_columns(pl.all().cast(pl.Float64, strict=False))
        .to_pandas()
        .corr(method="spearman")
        .loc[semantic_cols, behavior_cols]
    )

    fig, ax = plt.subplots(figsize=(10.8, 5.2))
    sns.heatmap(
        corr_df,
        vmin=-1,
        vmax=1,
        center=0,
        cmap=sns.diverging_palette(230, 35, s=75, l=55, as_cmap=True),
        annot=True,
        fmt=".2f",
        linewidths=0.7,
        linecolor=TOKENS["panel"],
        cbar_kws={"label": "Spearman rho"},
        ax=ax,
    )
    ax.set_xlabel("Existing user/tweet feature")
    ax.set_ylabel("Semantic repetition feature")
    ax.tick_params(axis="x", labelrotation=35)
    ax.tick_params(axis="y", labelrotation=0)

    add_chart_header(
        fig,
        ax,
        "Semantic repetition overlaps only partly with existing behavior features",
        "Spearman correlations on the 420 labeled users; values near zero suggest the semantic layer adds a different signal.",
    )
    output_path = output_dir / "semantic_behavior_correlations.png"
    save_figure(fig, output_path)
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate report-ready plots for semantic repetition features."
    )
    parser.add_argument("--labels-csv", type=Path, default=DEFAULT_LABELS_PATH)
    parser.add_argument("--semantic-features", type=Path, default=DEFAULT_SEMANTIC_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--feature-cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=(
            "Cache written by compare_semantic_features.py. Used only if the live DB "
            "load fails and --features-snapshot is omitted."
        ),
    )
    parser.add_argument(
        "--features-snapshot",
        type=Path,
        default=None,
        help=(
            "Optional explicit local labeled feature CSV fallback. Omit this for the "
            "live DB-backed comparison used in the report metrics."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    use_chart_theme()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    baseline_df, semantic_df = load_model_frames(
        args.labels_csv,
        args.semantic_features,
        args.features_snapshot,
        args.feature_cache_dir,
    )
    baseline_run = train_run(baseline_df)
    semantic_run = train_run(semantic_df)

    output_paths = [
        plot_metric_comparison(baseline_run, semantic_run, args.output_dir),
        plot_confusion_matrices(baseline_run, semantic_run, args.output_dir),
        plot_semantic_feature_distributions(semantic_df, args.output_dir),
        plot_pca_projection(semantic_df, semantic_run, args.output_dir),
        plot_semantic_behavior_correlations(semantic_df, args.output_dir),
    ]

    metrics = [
        {"run": "baseline", **baseline_run["metrics"]},
        {"run": "semantic", **semantic_run["metrics"]},
    ]
    print(pl.DataFrame(metrics))
    print("Wrote:")
    for path in output_paths:
        print(f"- {path}")


if __name__ == "__main__":
    main()
