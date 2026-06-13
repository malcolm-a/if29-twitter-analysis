# Semantic Repetition POC

This folder contains the Codex-assisted proof of concept for semantic tweet repetition features. It is intentionally separate from the existing supervised and unsupervised notebooks so the experiment can be cited separately in the report.

Build the labeled-user semantic sidecar:

```bash
uv run python -m src.etl.features.tweet_semantic_repetition \
  --labels-csv data/processed/labeling_sample.csv \
  --output data/processed/semantic_poc/tweet_semantic_repetition_features_labeled.parquet \
  --batch-size 128 \
  --chunk-size 10000 \
  --overwrite
```

Compare the current Random Forest with the semantic-enhanced feature set:

```bash
uv run python -m src.ml.semantic_poc.compare_semantic_features
```

This also writes a live-loaded cache to:

```text
data/processed/semantic_poc/live_feature_cache/
```

Generate the report plots:

```bash
uv run python -m src.ml.semantic_poc.plot_semantic_results
```

Plots are written to:

```text
docs/report/img/semantic_poc/
```

By default, the comparison and plotting scripts load the live feature matrix through `src.ml.features.get_labeled_features`, then left-join the semantic Parquet sidecar. If the plot script cannot open a second DB tunnel, it uses the live cache written by the comparison script. Use `--features-snapshot` only for exploratory offline plotting; snapshot metrics may not match the live DB-backed Random Forest comparison.
