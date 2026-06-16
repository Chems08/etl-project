"""Composant 3a — Producteur de streaming.

Publie des cotations boursières sur un topic Kafka.

Deux modes (variable d'environnement STREAM_MODE) :
  - live     : vraies cotations via le websocket Finnhub (wss://ws.finnhub.io)
  - simulate : ticks générés par marche aléatoire (démo hors heures de marché)

Message produit (JSON) :
  {"ticker": "AAPL", "price": 192.34, "volume": 120, "ts": 1718539200000}
"""
from __future__ import annotations

import json
import os
import random
import sys
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Rendre le package etl importable (pour lire les prix de départ depuis Postgres).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock_quotes")
MODE = os.getenv("STREAM_MODE", "live").lower()
TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "AAPL,MSFT,TSLA").split(",") if t.strip()]
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "")


def build_producer() -> KafkaProducer:
    """Crée le producteur Kafka, en réessayant le temps que le broker démarre."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                linger_ms=50,
            )
            print(f"[producer] Connecté à Kafka ({KAFKA_BOOTSTRAP}), topic='{TOPIC}'.")
            return producer
        except NoBrokersAvailable:
            print("[producer] Kafka indisponible, nouvelle tentative dans 5 s…")
            time.sleep(5)


def send(producer: KafkaProducer, ticker: str, price: float, volume: int, ts_ms: int) -> None:
    payload = {"ticker": ticker, "price": round(price, 4), "volume": int(volume), "ts": ts_ms}
    producer.send(TOPIC, key=ticker, value=payload)


# ─────────────────────────────── Mode LIVE ──────────────────────────────────
def run_live(producer: KafkaProducer) -> None:
    """Vraies cotations via le websocket Finnhub."""
    import websocket  # websocket-client

    if not FINNHUB_KEY:
        raise RuntimeError(
            "FINNHUB_API_KEY manquant. Renseignez-le dans .env "
            "ou lancez le producteur avec STREAM_MODE=simulate."
        )

    def on_open(ws):
        for ticker in TICKERS:
            ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))
        print(f"[producer/live] Abonné à {TICKERS} sur Finnhub.")

    def on_message(ws, message):
        msg = json.loads(message)
        if msg.get("type") != "trade":
            return
        for trade in msg.get("data", []):
            send(producer, trade["s"], trade["p"], trade.get("v", 0), trade["t"])

    def on_error(ws, error):
        print(f"[producer/live] Erreur websocket : {error}")

    def on_close(ws, *_):
        print("[producer/live] Websocket fermé, reconnexion dans 5 s…")
        time.sleep(5)
        connect()

    def connect():
        ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={FINNHUB_KEY}",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever()

    connect()


# ─────────────────────────────── Mode SIMULATE ──────────────────────────────
def seed_prices() -> dict[str, float]:
    """Prix de départ = dernier cours de clôture réel déjà chargé en base (core)."""
    prices: dict[str, float] = {}
    try:
        from etl.db import get_conn

        conn = get_conn(retries=3, delay=2)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ON (ticker) ticker, close "
                "FROM core.stock_prices ORDER BY ticker, trade_date DESC"
            )
            prices = {row[0]: float(row[1]) for row in cur.fetchall()}
        conn.close()
    except Exception as err:
        print(f"[producer/simulate] Lecture de core impossible ({err}).")

    # Valeur par défaut pour un ticker pas encore présent en base.
    for ticker in TICKERS:
        prices.setdefault(ticker, 100.0)
    print(f"[producer/simulate] Prix de départ (depuis core) : {prices}")
    return prices


def run_simulate(producer: KafkaProducer) -> None:
    """Génère des ticks par marche aléatoire (±0,15 % par pas)."""
    prices = seed_prices()
    print("[producer/simulate] Génération de ticks simulés (Ctrl+C pour arrêter).")
    while True:
        for ticker in TICKERS:
            drift = random.gauss(0, 0.0015)  # variation ~0,15 %
            prices[ticker] = max(1.0, prices[ticker] * (1 + drift))
            send(producer, ticker, prices[ticker], random.randint(10, 500), int(time.time() * 1000))
        producer.flush()
        time.sleep(1)


def main() -> None:
    producer = build_producer()
    if MODE == "simulate":
        run_simulate(producer)
    else:
        run_live(producer)


if __name__ == "__main__":
    main()
