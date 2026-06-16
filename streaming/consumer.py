"""Composant 3b — Consommateur de streaming.

Lit le topic Kafka des cotations, calcule des métriques glissantes et écrit dans
le sink PostgreSQL (schéma realtime) :

  - realtime.live_quotes : dernier prix + variation par ticker (upsert)
  - realtime.ohlc_1min   : bougies 1 minute agrégées en continu

Le flux alimente directement le dashboard temps réel.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Rendre le package etl importable quel que soit le répertoire de lancement.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from etl.db import get_conn, run_sql_file  # noqa: E402

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock_quotes")

UPSERT_QUOTE = """
    INSERT INTO realtime.live_quotes
        (ticker, price, prev_price, change_pct, volume, quote_ts, updated_at)
    VALUES (%(ticker)s, %(price)s, %(prev)s, %(change_pct)s, %(volume)s, %(quote_ts)s, now())
    ON CONFLICT (ticker) DO UPDATE SET
        price      = EXCLUDED.price,
        prev_price = EXCLUDED.prev_price,
        change_pct = EXCLUDED.change_pct,
        volume     = EXCLUDED.volume,
        quote_ts   = EXCLUDED.quote_ts,
        updated_at = now();
"""

UPSERT_OHLC = """
    INSERT INTO realtime.ohlc_1min
        (ticker, minute_ts, open, high, low, close, tick_count)
    VALUES (%(ticker)s, %(minute_ts)s, %(price)s, %(price)s, %(price)s, %(price)s, 1)
    ON CONFLICT (ticker, minute_ts) DO UPDATE SET
        high       = GREATEST(realtime.ohlc_1min.high, EXCLUDED.high),
        low        = LEAST(realtime.ohlc_1min.low, EXCLUDED.low),
        close      = EXCLUDED.close,
        tick_count = realtime.ohlc_1min.tick_count + 1;
"""


def ensure_schema() -> None:
    """Crée les schémas et les tables du sink si besoin (idempotent)."""
    run_sql_file(os.path.join(ROOT, "sql", "01_init.sql"))
    print("[consumer] Schéma realtime prêt.")


def build_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                group_id="stock-consumer",
                enable_auto_commit=True,
            )
            print(f"[consumer] Connecté à Kafka, lecture du topic '{TOPIC}'.")
            return consumer
        except NoBrokersAvailable:
            print("[consumer] Kafka indisponible, nouvelle tentative dans 5 s…")
            time.sleep(5)


def main() -> None:
    ensure_schema()
    consumer = build_consumer()
    conn = get_conn()
    conn.autocommit = True

    last_price: dict[str, float] = {}  # mémoire du dernier prix pour calculer la variation
    processed = 0

    for message in consumer:
        quote = message.value
        ticker = quote["ticker"]
        price = float(quote["price"])
        volume = int(quote.get("volume", 0))
        ts = datetime.fromtimestamp(quote["ts"] / 1000, tz=timezone.utc).replace(tzinfo=None)
        minute_ts = ts.replace(second=0, microsecond=0)

        prev = last_price.get(ticker, price)
        change_pct = round((price / prev - 1) * 100, 3) if prev else 0.0
        last_price[ticker] = price

        params = {
            "ticker": ticker,
            "price": price,
            "prev": prev,
            "change_pct": change_pct,
            "volume": volume,
            "quote_ts": ts,
            "minute_ts": minute_ts,
        }
        with conn.cursor() as cur:
            cur.execute(UPSERT_QUOTE, params)
            cur.execute(UPSERT_OHLC, params)

        processed += 1
        if processed % 50 == 0:
            print(f"[consumer] {processed} cotations traitées (dernier : {ticker} @ {price}).")


if __name__ == "__main__":
    main()
