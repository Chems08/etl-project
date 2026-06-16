"""Composant 1 — ETL Batch.

Ingestion des cours historiques (OHLCV journalier) via yfinance, nettoyage et
chargement idempotent dans PostgreSQL :

    yfinance  ->  staging.stock_prices_raw  ->  core.stock_prices (upsert)

Idempotent : relancer le batch ne crée pas de doublon (clé naturelle
(ticker, trade_date) + ON CONFLICT DO UPDATE).
"""
from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import yfinance as yf

# Import robuste, que le module soit lancé en script ou importé par Airflow.
try:
    from etl.db import get_conn, get_engine
except ModuleNotFoundError:  # exécution directe : python etl/batch_ingest.py
    from db import get_conn, get_engine


def get_tickers() -> list[str]:
    raw = os.getenv("TICKERS", "AAPL,MSFT,TSLA,AMZN,GOOGL,NVDA,META,JPM")
    return [t.strip().upper() for t in raw.split(",") if t.strip()]


def extract(tickers: list[str], period: str = "6mo") -> pd.DataFrame:
    """Télécharge l'historique journalier pour chaque ticker."""
    print(f"[etl] Extraction yfinance pour {tickers} (période={period})")
    raw = yf.download(
        tickers=tickers,
        period=period,
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )

    # yfinance renvoie des colonnes multi-index (ticker, champ) -> on aplatit.
    frames = []
    for ticker in tickers:
        if ticker not in raw.columns.get_level_values(0):
            print(f"[etl] Aucune donnée pour {ticker}, ignoré.")
            continue
        df = raw[ticker].copy()
        df["ticker"] = ticker
        frames.append(df.reset_index())

    if not frames:
        raise RuntimeError("Aucune donnée téléchargée — vérifier les tickers / le réseau.")

    return pd.concat(frames, ignore_index=True)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyage : renommage, cast de types, suppression des NaN, déduplication."""
    df = df.rename(
        columns={
            "Date": "trade_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )

    cols = ["ticker", "trade_date", "open", "high", "low", "close", "adj_close", "volume"]
    df = df[[c for c in cols if c in df.columns]].copy()

    # Cast de types explicite.
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    for col in ["open", "high", "low", "close", "adj_close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    # Nettoyage : lignes sans prix de clôture, puis déduplication sur la clé naturelle.
    df = df.dropna(subset=["close"])
    df = df.drop_duplicates(subset=["ticker", "trade_date"], keep="last")

    df["ingested_at"] = datetime.utcnow()

    # Garde-fou : un téléchargement vide ne doit pas corrompre la table de staging
    # (sinon pandas recrée des colonnes en TEXT et l'upsert vers core casse).
    if df.empty:
        raise RuntimeError(
            "0 ligne après nettoyage — téléchargement échoué (rate-limit Yahoo ?). "
            "On interrompt l'ETL pour préserver le schéma de staging."
        )

    print(f"[etl] {len(df)} lignes après nettoyage.")
    return df


def load(df: pd.DataFrame) -> None:
    """Charge en staging (remplacement) puis upsert idempotent dans core.stock_prices."""
    engine = get_engine()

    # 1) Couche de staging : on remplace le contenu brut du dernier run.
    df.to_sql(
        "stock_prices_raw",
        engine,
        schema="staging",
        if_exists="replace",
        index=False,
    )
    print("[etl] staging.stock_prices_raw chargé.")

    # 2) Upsert depuis staging vers core (idempotent).
    # Cast explicite dans le SELECT : robuste même si pandas a inféré des colonnes
    # en TEXT côté staging (téléchargement partiel, etc.).
    upsert = """
        INSERT INTO core.stock_prices
            (ticker, trade_date, open, high, low, close, adj_close, volume, ingested_at)
        SELECT
            ticker::text,
            trade_date::date,
            open::double precision,
            high::double precision,
            low::double precision,
            close::double precision,
            adj_close::double precision,
            volume::bigint,
            ingested_at::timestamp
        FROM staging.stock_prices_raw
        ON CONFLICT (ticker, trade_date) DO UPDATE SET
            open       = EXCLUDED.open,
            high       = EXCLUDED.high,
            low        = EXCLUDED.low,
            close      = EXCLUDED.close,
            adj_close  = EXCLUDED.adj_close,
            volume     = EXCLUDED.volume,
            ingested_at = EXCLUDED.ingested_at;
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(upsert)
            affected = cur.rowcount
        conn.commit()
        print(f"[etl] core.stock_prices : {affected} lignes insérées/mises à jour (idempotent).")
    finally:
        conn.close()


def run() -> None:
    tickers = get_tickers()
    df = extract(tickers)
    df = transform(df)
    load(df)
    print("[etl] ETL batch terminé.")


if __name__ == "__main__":
    run()
