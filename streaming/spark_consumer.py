"""Composant 3b — Traitement de flux avec Spark Structured Streaming.

Remplace l'ancien consommateur Python pur par un job Spark distribué :

  Kafka (topic stock_quotes) → Spark Structured Streaming → PostgreSQL (realtime.*)

Pour chaque micro-batch, Spark :
  - parse le JSON des cotations ;
  - agrège des bougies OHLC à la minute (open/high/low/close + nb de ticks)
    via des fonctions de fenêtrage ;
  - écrit les résultats dans Postgres (upsert) :
      · realtime.live_quotes : dernier prix + variation par ticker
      · realtime.ohlc_1min   : bougies 1 minute agrégées en continu

L'écriture se fait dans `foreachBatch` avec psycopg2 pour conserver la
sémantique d'upsert (ON CONFLICT) déjà utilisée par le projet.
"""
from __future__ import annotations

import os
import sys
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# Rendre le package etl importable (helpers de connexion + DDL partagés).
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from etl.db import get_conn, run_sql_file  # noqa: E402

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock_quotes")
TRIGGER = os.getenv("SPARK_TRIGGER", "5 seconds")

# Schéma des messages produits par streaming/producer.py
QUOTE_SCHEMA = StructType(
    [
        StructField("ticker", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", LongType()),
        StructField("ts", LongType()),  # epoch millisecondes
    ]
)

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

# OHLC pré-agrégé par Spark : on fusionne le batch courant avec la bougie existante.
UPSERT_OHLC = """
    INSERT INTO realtime.ohlc_1min
        (ticker, minute_ts, open, high, low, close, tick_count)
    VALUES (%(ticker)s, %(minute_ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(tick_count)s)
    ON CONFLICT (ticker, minute_ts) DO UPDATE SET
        high       = GREATEST(realtime.ohlc_1min.high, EXCLUDED.high),
        low        = LEAST(realtime.ohlc_1min.low, EXCLUDED.low),
        close      = EXCLUDED.close,
        tick_count = realtime.ohlc_1min.tick_count + EXCLUDED.tick_count;
"""


def ensure_schema() -> None:
    """Crée les schémas et tables du sink si besoin (idempotent)."""
    run_sql_file(os.path.join(ROOT, "sql", "01_init.sql"))
    print("[spark] Schéma realtime prêt.")


def ensure_topic() -> None:
    """Crée le topic Kafka s'il n'existe pas encore.

    Spark échoue au démarrage si le topic est absent ; on le crée donc ici pour
    être indépendant de l'ordre de démarrage producer/spark.
    """
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

    for _ in range(30):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
            break
        except NoBrokersAvailable:
            print("[spark] Kafka indisponible, nouvelle tentative dans 5 s…")
            time.sleep(5)
    else:
        raise RuntimeError("Kafka injoignable pour créer le topic.")

    try:
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)])
        print(f"[spark] Topic '{TOPIC}' créé.")
    except TopicAlreadyExistsError:
        print(f"[spark] Topic '{TOPIC}' déjà présent.")
    finally:
        admin.close()


def aggregate_ohlc(batch: DataFrame) -> DataFrame:
    """Bougies OHLC à la minute pour le micro-batch courant."""
    w_open = Window.partitionBy("ticker", "minute_ts").orderBy(F.col("ts").asc())
    w_close = Window.partitionBy("ticker", "minute_ts").orderBy(F.col("ts").desc())
    return (
        batch.withColumn("open", F.first("price").over(w_open))
        .withColumn("close", F.first("price").over(w_close))
        .groupBy("ticker", "minute_ts")
        .agg(
            F.first("open").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.first("close").alias("close"),
            F.count(F.lit(1)).alias("tick_count"),
        )
    )


def latest_quotes(batch: DataFrame) -> DataFrame:
    """Dernier tick par ticker dans le micro-batch courant."""
    w_last = Window.partitionBy("ticker").orderBy(F.col("ts").desc())
    return (
        batch.withColumn("rn", F.row_number().over(w_last))
        .filter(F.col("rn") == 1)
        .select("ticker", "price", "volume", F.col("event_time").alias("quote_ts"))
    )


def process_batch(batch_df: DataFrame, epoch_id: int) -> None:
    """Appelé par Spark pour chaque micro-batch : upsert dans Postgres."""
    if batch_df.isEmpty():
        return

    batch = batch_df.withColumn("minute_ts", F.date_trunc("minute", F.col("event_time")))
    ohlc_rows = aggregate_ohlc(batch).collect()
    latest_rows = latest_quotes(batch).collect()

    conn = get_conn()
    conn.autocommit = True
    try:
        # Prix précédents connus → base de calcul de la variation.
        with conn.cursor() as cur:
            cur.execute("SELECT ticker, price FROM realtime.live_quotes")
            prev_price = {t: float(p) for t, p in cur.fetchall()}

        with conn.cursor() as cur:
            for r in ohlc_rows:
                cur.execute(
                    UPSERT_OHLC,
                    {
                        "ticker": r["ticker"],
                        "minute_ts": r["minute_ts"],
                        "open": r["open"],
                        "high": r["high"],
                        "low": r["low"],
                        "close": r["close"],
                        "tick_count": int(r["tick_count"]),
                    },
                )
            for r in latest_rows:
                price = float(r["price"])
                prev = prev_price.get(r["ticker"], price)
                change_pct = round((price / prev - 1) * 100, 3) if prev else 0.0
                cur.execute(
                    UPSERT_QUOTE,
                    {
                        "ticker": r["ticker"],
                        "price": price,
                        "prev": prev,
                        "change_pct": change_pct,
                        "volume": int(r["volume"] or 0),
                        "quote_ts": r["quote_ts"],
                    },
                )
    finally:
        conn.close()

    print(f"[spark] batch {epoch_id} : {len(latest_rows)} tickers, {len(ohlc_rows)} bougies.")


def main() -> None:
    ensure_schema()
    ensure_topic()

    spark = (
        SparkSession.builder.appName("stock-stream-processor")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    quotes = (
        raw.select(F.from_json(F.col("value").cast("string"), QUOTE_SCHEMA).alias("q"))
        .select("q.*")
        .withColumn("event_time", (F.col("ts") / 1000).cast("timestamp"))
    )

    print(f"[spark] Lecture du topic '{TOPIC}' sur {KAFKA_BOOTSTRAP} (trigger={TRIGGER}).")
    query = (
        quotes.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", os.getenv("SPARK_CHECKPOINT", "/tmp/spark-checkpoint"))
        .trigger(processingTime=TRIGGER)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
