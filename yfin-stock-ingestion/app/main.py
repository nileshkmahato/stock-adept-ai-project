# --------------------------------------------------
# STOCK INGESTION JOB
# --------------------------------------------------
# This script automates the ingestion of stock market data using Yahoo Finance.
# It loads configuration from `config.json`, downloads both historical and daily
# price data for specified tickers, standardizes the format, and saves the results
# into Google Cloud Storage (GCS) in a structured folder layout.
# Historical data is fetched once (if not already present), while daily data is
# ingested for the latest trading day. The job ensures clean, deduplicated,
# and time-sorted records for downstream analysis.
# --------------------------------------------------


import os
import json
from datetime import datetime
import pandas as pd
import yfinance as yf
import gcsfs


# --------------------------------------------------
# CONFIG
# --------------------------------------------------
def load_config():

    with open("config.json") as f:
        cfg = json.load(f)

    return {
        "TICKERS": cfg["TICKERS"],
        "HISTORICAL_START_DATE": cfg["HISTORICAL_START_DATE"],
        "GCS_BUCKET": cfg["GCS_BUCKET"],
        "BASE_PATH": cfg.get("BASE_PATH", "data_raw"),
    }


config = load_config()

TICKERS = config["TICKERS"]
HISTORICAL_START_DATE = config["HISTORICAL_START_DATE"]
GCS_BUCKET = config["GCS_BUCKET"]
BASE_PATH = config["BASE_PATH"]

fs = gcsfs.GCSFileSystem()


# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def gcs_path(*paths) -> str:
    return f"{GCS_BUCKET}/" + "/".join(paths)


def gcs_exists(path: str) -> bool:
    return fs.exists(path)


def standardize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("Empty DataFrame")

    df = df.reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df = df.rename(columns={
        "Date": "timestamp",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    return (
        df[["timestamp", "open", "high", "low", "close", "volume"]]
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


# --------------------------------------------------
# INGESTION
# --------------------------------------------------
def ingest_historical(yf_symbol: str, ticker: str):
    hist_file = gcs_path(
        BASE_PATH,
        f"stock_symbol={ticker}",
        "data_type_label=historical",
        f"{ticker.lower()}_h_{HISTORICAL_START_DATE.replace('-', '')}.csv",
    )

    if gcs_exists(hist_file):
        print(f"[SKIP] Historical exists → {ticker}")
        return

    print(f"[INFO] Downloading historical → {ticker}")

    df = yf.download(
        yf_symbol,
        start=HISTORICAL_START_DATE,
        progress=False,
        auto_adjust=False,
    )

    if df.empty:
        print(f"[ERROR] No historical data → {ticker}")
        return

    df = standardize(df)

    with fs.open(hist_file, "w") as f:
        df.to_csv(f, index=False)

    print(f"[OK] Historical saved → {hist_file}")


def ingest_daily(yf_symbol: str, ticker: str):
    df = yf.download(
        yf_symbol,
        period="2d",
        progress=False,
        auto_adjust=False,
    )

    if df.empty:
        print(f"[WARN] No daily data → {ticker}")
        return

    df = standardize(df)
    latest = df.iloc[-1:]

    trade_date = latest["timestamp"].dt.strftime("%Y%m%d").iloc[0]

    daily_file = gcs_path(
        BASE_PATH,
        f"stock_symbol={ticker}",
        "data_type_label=daily",
        f"{ticker.lower()}_d_{trade_date}.csv",
    )

    if gcs_exists(daily_file):
        print(f"[SKIP] Daily exists → {ticker} ({trade_date})")
        return

    with fs.open(daily_file, "w") as f:
        latest.to_csv(f, index=False)

    print(f"[OK] Daily saved → {daily_file}")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("🚀 STOCK INGESTION JOB STARTED")

    for yf_symbol in TICKERS:
        ticker = yf_symbol.split(".")[0]
        print(f"\n📊 Processing {ticker}")

        ingest_historical(yf_symbol, ticker)
        ingest_daily(yf_symbol, ticker)

    print("\n✅ JOB COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
