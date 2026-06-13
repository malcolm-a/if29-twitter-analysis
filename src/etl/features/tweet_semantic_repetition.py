from __future__ import annotations

import argparse
import math
import os
import re
import shutil
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import numpy as np
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from src.db import get_db_connection

DEFAULT_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
DEFAULT_OUTPUT_PATH = Path("data/processed/tweet_semantic_repetition_features.parquet")
DEFAULT_RANDOM_STATE = 1
RESULT_BUFFER_ROWS = 50_000

URL_RE = re.compile(r"https?://\S+|www\.\S+", flags=re.IGNORECASE)
MENTION_RE = re.compile(r"@\w+")
RETWEET_RE = re.compile(r"^\s*RT\s+", flags=re.IGNORECASE)
SPACE_RE = re.compile(r"\s+")

FEATURE_SCHEMA: dict[str, pl.DataType] = {
    "user_id": pl.Int64,
    "semantic_tweet_count": pl.Int64,
    "semantic_pair_count": pl.Int64,
    "exact_duplicate_rate": pl.Float64,
    "normalized_duplicate_rate": pl.Float64,
    "unique_normalized_tweet_ratio": pl.Float64,
    "semantic_similarity_mean": pl.Float64,
    "semantic_similarity_max": pl.Float64,
    "semantic_similarity_std": pl.Float64,
    "semantic_similarity_p90": pl.Float64,
    "semantic_similarity_over_080_rate": pl.Float64,
    "semantic_similarity_over_090_rate": pl.Float64,
    "similarity_sample_size": pl.Int64,
    "similarity_is_sampled": pl.Boolean,
    "semantic_model_name": pl.Utf8,
}


def normalize_lexical_text(text: str | None) -> str:
    """Normalize tweet text for exact lexical repetition counts."""
    if text is None:
        return ""

    normalized = RETWEET_RE.sub("", text)
    normalized = URL_RE.sub("<url>", normalized)
    normalized = MENTION_RE.sub("<user>", normalized)
    normalized = SPACE_RE.sub(" ", normalized)
    return normalized.strip().lower()


def normalize_semantic_text(text: str | None) -> str:
    """Normalize tweet text before embedding while preserving useful casing."""
    if text is None:
        return ""

    normalized = RETWEET_RE.sub("", text)
    normalized = URL_RE.sub("<url>", normalized)
    normalized = MENTION_RE.sub("<user>", normalized)
    normalized = SPACE_RE.sub(" ", normalized)
    return normalized.strip()


def select_device(device: str) -> str:
    if device != "auto":
        return device

    try:
        import torch
    except ImportError:
        return "cpu"

    if torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def load_sentence_transformer(model_name: str, device: str):
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise RuntimeError(
            "sentence-transformers is not installed. Run `uv sync` after updating "
            "pyproject.toml, then rerun this command."
        ) from exc

    resolved_device = select_device(device)
    return SentenceTransformer(model_name, device=resolved_device), resolved_device


@dataclass
class UserSemanticAccumulator:
    user_id: int
    model_name: str
    sample_threshold: int
    random_state: int = DEFAULT_RANDOM_STATE
    count: int = 0
    exact_text_counts: Counter[str] = field(default_factory=Counter)
    normalized_text_counts: Counter[str] = field(default_factory=Counter)
    embedding_sum: np.ndarray | None = None
    sample_embeddings: list[np.ndarray] = field(default_factory=list)
    _rng: np.random.Generator | None = None

    def add_tweet(self, raw_text: str, lexical_text: str, embedding: np.ndarray) -> None:
        if not lexical_text:
            return

        self.count += 1
        self.exact_text_counts[raw_text.strip()] += 1
        self.normalized_text_counts[lexical_text] += 1

        embedding = np.asarray(embedding, dtype=np.float32)
        if self.embedding_sum is None:
            self.embedding_sum = embedding.astype(np.float64)
        else:
            self.embedding_sum += embedding

        if len(self.sample_embeddings) < self.sample_threshold:
            self.sample_embeddings.append(embedding)
            return

        rng = self._get_rng()
        replacement_index = int(rng.integers(0, self.count))
        if replacement_index < self.sample_threshold:
            self.sample_embeddings[replacement_index] = embedding

    def to_feature_row(self) -> dict[str, object]:
        pair_count = self.count * (self.count - 1) // 2
        exact_duplicate_rate = duplicate_rate(self.exact_text_counts, self.count)
        normalized_duplicate_rate = duplicate_rate(self.normalized_text_counts, self.count)
        unique_normalized_ratio = unique_ratio(self.normalized_text_counts, self.count)

        row: dict[str, object] = {
            "user_id": self.user_id,
            "semantic_tweet_count": self.count,
            "semantic_pair_count": pair_count,
            "exact_duplicate_rate": exact_duplicate_rate,
            "normalized_duplicate_rate": normalized_duplicate_rate,
            "unique_normalized_tweet_ratio": unique_normalized_ratio,
            "semantic_similarity_mean": None,
            "semantic_similarity_max": None,
            "semantic_similarity_std": None,
            "semantic_similarity_p90": None,
            "semantic_similarity_over_080_rate": None,
            "semantic_similarity_over_090_rate": None,
            "similarity_sample_size": min(self.count, len(self.sample_embeddings)),
            "similarity_is_sampled": self.count > self.sample_threshold,
            "semantic_model_name": self.model_name,
        }

        if self.count < 2 or self.embedding_sum is None:
            return row

        row["semantic_similarity_mean"] = mean_pairwise_cosine(self.embedding_sum, self.count)

        distribution = pairwise_similarity_distribution(self.sample_embeddings)
        row.update(distribution)
        return row

    def _get_rng(self) -> np.random.Generator:
        if self._rng is None:
            seed = (self.random_state + self.user_id) % (2**32 - 1)
            self._rng = np.random.default_rng(seed)
        return self._rng


class ParquetResultWriter:
    def __init__(self, output_path: Path, overwrite: bool = False) -> None:
        self.output_path = output_path
        self.overwrite = overwrite
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.parts_dir = self.output_path.with_name(f"{self.output_path.stem}_parts")
        self.buffer: list[dict[str, object]] = []
        self.parts: list[Path] = []

        if self.output_path.exists() and not overwrite:
            raise FileExistsError(
                f"{self.output_path} already exists. Pass --overwrite to replace it."
            )
        if self.parts_dir.exists():
            if not overwrite:
                raise FileExistsError(
                    f"{self.parts_dir} already exists. Pass --overwrite to replace it."
                )
            shutil.rmtree(self.parts_dir)

        self.parts_dir.mkdir(parents=True, exist_ok=True)

    def add_row(self, row: dict[str, object]) -> None:
        self.buffer.append(row)
        if len(self.buffer) >= RESULT_BUFFER_ROWS:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return

        part_path = self.parts_dir / f"part-{len(self.parts):05d}.parquet"
        df = rows_to_dataframe(self.buffer)
        df.write_parquet(part_path)
        self.parts.append(part_path)
        self.buffer = []

    def finish(self) -> None:
        self.flush()

        if not self.parts:
            rows_to_dataframe([]).write_parquet(self.output_path)
        elif len(self.parts) == 1:
            self.parts[0].replace(self.output_path)
        else:
            pattern = str(self.parts_dir / "part-*.parquet")
            pl.scan_parquet(pattern).sink_parquet(self.output_path)

        shutil.rmtree(self.parts_dir)


def duplicate_rate(counts: Counter[str], total: int) -> float | None:
    if total == 0:
        return None
    return (total - len(counts)) / total


def unique_ratio(counts: Counter[str], total: int) -> float | None:
    if total == 0:
        return None
    return len(counts) / total


def mean_pairwise_cosine(embedding_sum: np.ndarray, count: int) -> float | None:
    if count < 2:
        return None

    numerator = float(np.dot(embedding_sum, embedding_sum) - count)
    denominator = count * (count - 1)
    value = numerator / denominator
    return float(np.clip(value, -1.0, 1.0))


def pairwise_similarity_distribution(embeddings: list[np.ndarray]) -> dict[str, float | None]:
    count = len(embeddings)
    if count < 2:
        return {
            "semantic_similarity_max": None,
            "semantic_similarity_std": None,
            "semantic_similarity_p90": None,
            "semantic_similarity_over_080_rate": None,
            "semantic_similarity_over_090_rate": None,
        }

    matrix = np.vstack(embeddings).astype(np.float32)
    similarities = matrix @ matrix.T
    upper_triangle = np.triu_indices(count, k=1)
    pairwise = similarities[upper_triangle]

    return {
        "semantic_similarity_max": float(np.max(pairwise)),
        "semantic_similarity_std": float(np.std(pairwise)),
        "semantic_similarity_p90": float(np.quantile(pairwise, 0.90)),
        "semantic_similarity_over_080_rate": float(np.mean(pairwise >= 0.80)),
        "semantic_similarity_over_090_rate": float(np.mean(pairwise >= 0.90)),
    }


def rows_to_dataframe(rows: list[dict[str, object]]) -> pl.DataFrame:
    if rows:
        return pl.DataFrame(rows).cast(FEATURE_SCHEMA)
    return pl.DataFrame(schema=FEATURE_SCHEMA)


def build_query(limit: int | None = None, filter_user_ids: bool = False) -> str:
    query = """
        select
            (raw_data->'user'->>'id')::bigint as user_id,
            id as tweet_id,
            created_at,
            tweet_text,
            raw_data->>'lang' as lang
        from tweets
        where tweet_text is not null
    """
    if filter_user_ids:
        query += "\n        and (raw_data->'user'->>'id')::bigint = any(%s)"

    query += """
        order by (raw_data->'user'->>'id')::bigint, created_at, id
    """
    if limit is not None:
        query += "\n        limit %s"
    return query


def iter_tweets(
    chunk_size: int,
    limit: int | None = None,
    user_ids: list[int] | None = None,
) -> Iterable[list[tuple]]:
    params: list[object] = []
    if user_ids is not None:
        params.append(user_ids)
    if limit is not None:
        params.append(limit)

    with get_db_connection() as conn:
        with conn.cursor(name="tweet_semantic_repetition_cursor") as cursor:
            cursor.itersize = chunk_size
            query = build_query(limit=limit, filter_user_ids=user_ids is not None)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            while rows := cursor.fetchmany(chunk_size):
                yield rows


def encode_texts(model, texts: list[str], batch_size: int) -> np.ndarray:
    return model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=False,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )


def process_tweet_rows(
    rows: list[tuple],
    embeddings: np.ndarray,
    accumulators: dict[int, UserSemanticAccumulator],
    writer: ParquetResultWriter,
    model_name: str,
    sample_threshold: int,
    random_state: int,
) -> tuple[int | None, int]:
    last_user_id: int | None = None
    usable_rows = 0

    for row, embedding in zip(rows, embeddings, strict=True):
        user_id = int(row[0])
        raw_text = row[3] or ""
        lexical_text = normalize_lexical_text(raw_text)
        if not lexical_text:
            continue

        accumulator = accumulators.get(user_id)
        if accumulator is None:
            accumulator = UserSemanticAccumulator(
                user_id=user_id,
                model_name=model_name,
                sample_threshold=sample_threshold,
                random_state=random_state,
            )
            accumulators[user_id] = accumulator

        accumulator.add_tweet(raw_text, lexical_text, embedding)
        last_user_id = user_id
        usable_rows += 1

    if last_user_id is not None:
        finalizable_user_ids = [user_id for user_id in accumulators if user_id != last_user_id]
        for user_id in finalizable_user_ids:
            writer.add_row(accumulators.pop(user_id).to_feature_row())

    return last_user_id, usable_rows


def build_semantic_repetition_features(
    model_name: str,
    output_path: Path,
    chunk_size: int,
    batch_size: int,
    sample_threshold: int,
    device: str,
    limit: int | None,
    user_ids: list[int] | None,
    overwrite: bool,
    random_state: int = DEFAULT_RANDOM_STATE,
) -> None:
    model, resolved_device = load_sentence_transformer(model_name, device)
    writer = ParquetResultWriter(output_path, overwrite=overwrite)
    accumulators: dict[int, UserSemanticAccumulator] = {}
    processed_tweets = 0
    usable_tweets = 0
    start_time = time.perf_counter()

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    progress = tqdm(total=limit, unit="tweet") if tqdm is not None and limit else None

    print(
        f"Encoding tweets with {model_name} on {resolved_device}. "
        f"chunk_size={chunk_size}, batch_size={batch_size}, sample_threshold={sample_threshold}"
    )
    if user_ids is not None:
        print(f"Filtering to {len(user_ids):,} user IDs.")

    for rows in iter_tweets(chunk_size=chunk_size, limit=limit, user_ids=user_ids):
        semantic_texts = [normalize_semantic_text(row[3]) for row in rows]
        embeddings = encode_texts(model, semantic_texts, batch_size=batch_size)
        _last_user_id, chunk_usable = process_tweet_rows(
            rows=rows,
            embeddings=embeddings,
            accumulators=accumulators,
            writer=writer,
            model_name=model_name,
            sample_threshold=sample_threshold,
            random_state=random_state,
        )
        processed_tweets += len(rows)
        usable_tweets += chunk_usable
        if progress is not None:
            progress.update(len(rows))
        elif processed_tweets % (chunk_size * 10) == 0:
            elapsed = max(time.perf_counter() - start_time, 1e-9)
            print(
                f"processed={processed_tweets:,}, usable={usable_tweets:,}, "
                f"tweets_per_second={processed_tweets / elapsed:,.1f}"
            )

    if progress is not None:
        progress.close()

    for accumulator in accumulators.values():
        writer.add_row(accumulator.to_feature_row())
    writer.finish()

    elapsed = max(time.perf_counter() - start_time, 1e-9)
    print(
        f"Wrote {output_path} from {processed_tweets:,} tweets "
        f"({usable_tweets:,} usable) in {elapsed / 60:.1f} min "
        f"({processed_tweets / elapsed:,.1f} tweets/sec)."
    )


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def load_user_ids_from_labels(csv_path: Path) -> list[int]:
    labels = pl.read_csv(csv_path, separator=";")
    if "user_id" not in labels.columns:
        raise ValueError(f"{csv_path} does not contain a user_id column.")

    if "label" in labels.columns:
        labels = (
            labels.with_columns(pl.col("label").cast(pl.Utf8))
            .filter(pl.col("label").is_in(["0", "1", "0,0"]))
            .with_columns(pl.col("label").replace({"0,0": "0"}).cast(pl.Int8))
        )

    return (
        labels.select("user_id")
        .drop_nulls()
        .with_columns(pl.col("user_id").cast(pl.Int64))
        .unique()
        .sort("user_id")
        .get_column("user_id")
        .to_list()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build per-user semantic repetition features from all tweets."
    )
    parser.add_argument("--model", default=DEFAULT_MODEL_NAME)
    parser.add_argument("--chunk-size", type=positive_int, default=50_000)
    parser.add_argument("--batch-size", type=positive_int, default=256)
    parser.add_argument("--sample-threshold", type=positive_int, default=1_000)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--device", default="auto", choices=["auto", "cpu", "mps"])
    parser.add_argument("--limit", type=positive_int, default=None)
    parser.add_argument(
        "--labels-csv",
        type=Path,
        default=None,
        help="Optional semicolon-separated label CSV. If set, only tweets from its user_id column are processed.",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--random-state", type=int, default=DEFAULT_RANDOM_STATE)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.sample_threshold > 5_000:
        raise ValueError("sample_threshold above 5000 can create large pairwise matrices.")
    if math.prod([args.sample_threshold, args.sample_threshold]) > 25_000_000:
        raise ValueError("sample_threshold creates a pairwise matrix above 25M cells.")

    user_ids = load_user_ids_from_labels(args.labels_csv) if args.labels_csv else None

    build_semantic_repetition_features(
        model_name=args.model,
        output_path=args.output,
        chunk_size=args.chunk_size,
        batch_size=args.batch_size,
        sample_threshold=args.sample_threshold,
        device=args.device,
        limit=args.limit,
        user_ids=user_ids,
        overwrite=args.overwrite,
        random_state=args.random_state,
    )


if __name__ == "__main__":
    main()
