from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import polars as pl
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from src.ml.features import get_labeled_features

DEFAULT_LABELS_PATH = Path("data/processed/labeling_sample.csv")
DEFAULT_SEMANTIC_PATH = Path(
    "data/processed/semantic_poc/tweet_semantic_repetition_features_labeled.parquet"
)
DEFAULT_CACHE_DIR = Path("data/processed/semantic_poc/live_feature_cache")

DROP_COLS = [
    "user_id",
    "screen_name",
    "name",
    "created_at",
    "last_tweet_at",
    "first_seen_at",
    "last_seen_at",
    "profile_url",
    "description",
    "semantic_model_name",
]

LOG_COLS = [
    "followers_count",
    "friends_count",
    "listed_count",
    "favourites_count",
    "statuses_count",
    "tweet_count",
    "semantic_tweet_count",
    "semantic_pair_count",
]

BEST_RF_PARAMS = {
    "class_weight": "balanced",
    "max_depth": None,
    "max_features": "sqrt",
    "min_samples_leaf": 1,
    "min_samples_split": 10,
    "n_estimators": 200,
}


def prepare_xy(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.Series, list[str]]:
    feature_cols = [column for column in df.columns if column not in DROP_COLS + ["label"]]
    x = df.select(feature_cols)
    x = x.select(pl.exclude(pl.Utf8))
    x = x.with_columns(pl.col(pl.Boolean).cast(pl.Int8)).fill_null(0)

    existing_log_cols = [column for column in LOG_COLS if column in x.columns]
    if existing_log_cols:
        x = x.with_columns(pl.col(existing_log_cols).log1p())

    return x, df["label"], x.columns


def evaluate(df: pl.DataFrame) -> dict[str, float]:
    x, y, _feature_cols = prepare_xy(df)
    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=0.2,
        random_state=1,
        stratify=y.to_numpy(),
    )

    model = RandomForestClassifier(random_state=1, **BEST_RF_PARAMS)
    model.fit(x_train.to_numpy(), y_train.to_numpy())
    predictions = model.predict(x_test.to_numpy())

    return {
        "accuracy": accuracy_score(y_test.to_numpy(), predictions),
        "precision": precision_score(y_test.to_numpy(), predictions, zero_division=0),
        "recall": recall_score(y_test.to_numpy(), predictions, zero_division=0),
        "f1": f1_score(y_test.to_numpy(), predictions, zero_division=0),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare the supervised random forest with and without semantic features."
    )
    parser.add_argument("--labels-csv", type=Path, default=DEFAULT_LABELS_PATH)
    parser.add_argument("--semantic-features", type=Path, default=DEFAULT_SEMANTIC_PATH)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help="Directory where the live-loaded baseline and semantic feature matrices are cached.",
    )
    return parser.parse_args()


def write_live_cache(
    cache_dir: Path,
    baseline: pl.DataFrame,
    semantic: pl.DataFrame,
    metrics: list[dict[str, float | str]],
) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    baseline.write_parquet(cache_dir / "baseline_features.parquet")
    semantic.write_parquet(cache_dir / "semantic_features.parquet")
    (cache_dir / "metrics.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    baseline = get_labeled_features(str(args.labels_csv)).drop_nans("label")
    semantic = get_labeled_features(
        str(args.labels_csv),
        include_semantic=True,
        semantic_features_path=args.semantic_features,
    ).drop_nans("label")

    if baseline.height != semantic.height:
        raise ValueError(
            f"Row count changed after semantic join: baseline={baseline.height}, "
            f"semantic={semantic.height}"
        )

    baseline_metrics = evaluate(baseline)
    semantic_metrics = evaluate(semantic)

    rows = [
        {"run": "baseline", **baseline_metrics},
        {"run": "semantic", **semantic_metrics},
    ]
    if args.cache_dir is not None:
        write_live_cache(args.cache_dir, baseline, semantic, rows)
    print(pl.DataFrame(rows))


if __name__ == "__main__":
    main()
