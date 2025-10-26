# models/cv_prices_with_ema.py
# Requires: dbt-bigquery with Python models enabled
# Depends on: models/cv_prices_sessionized.sql

import pandas as pd

def model(dbt, session):
    # Configure how the table is built in BigQuery
    dbt.config(
        materialized="table",
        # Optional: Partition + cluster for faster time-series work
        partition_by={"field": "price_timestamp", "data_type": "timestamp"},
        cluster_by=["chain", "token_chain_id", "token_address"]
    )

    # Pull the upstream relation produced by your SQL model
    src = dbt.ref("cv_prep_ema21_ema50")

    # Convert to pandas for vectorized EMA computation
    # (dbt-bigquery Python models expose a BigQuery DataFrame; to_pandas() materializes it)
    df = src.to_pandas()

    # --- hygiene & ordering ---
    # ensure expected columns exist
    expected_cols = {
        "chain", "token_chain_id", "token_address",
        "price_timestamp", "price_usd", "volume", "mktcap"
    }
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns from upstream model: {missing}")

    # Drop rows without price or timestamp; dedupe identical keys/timestamps
    df = df.dropna(subset=["price_timestamp", "price_usd"]).copy()
    df["price_timestamp"] = pd.to_datetime(df["price_timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["price_timestamp"])
    df = df.sort_values(["chain", "token_chain_id", "token_address", "price_timestamp"])
    df = df.drop_duplicates(
        subset=["chain", "token_chain_id", "token_address", "price_timestamp"],
        keep="last"
    )

    # --- EMA computation ---
    # Pandas' ewm uses order only (not time deltas). That’s the standard trading EMA.
    # adjust=False -> classic recursive EMA: ema_t = alpha*price_t + (1-alpha)*ema_{t-1}
    def add_emas(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        g["ema_21"] = g["price_usd"].ewm(span=21, adjust=False, min_periods=1).mean()
        g["ema_50"] = g["price_usd"].ewm(span=50, adjust=False, min_periods=1).mean()
        return g

    df = df.groupby(["chain", "token_chain_id", "token_address"], group_keys=False).apply(add_emas)

    # (Optional) keep a lean column set; add more if you need them
    out_cols = [
        "chain", "token_chain_id", "token_address",
        "price_timestamp", "price_usd", "volume", "mktcap",
        "ema_21", "ema_50"
    ]
    df = df[out_cols]

    # BigQuery likes native types: pandas -> Arrow -> BigQuery handled by dbt
    return df
