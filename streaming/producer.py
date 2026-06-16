"""Composant 3a — Producteur de streaming.

Publie des cotations boursières temps réel sur un topic Kafka, via le
websocket Finnhub (wss://ws.finnhub.io).

Message produit (JSON) :
  {"ticker": "AAPL", "price": 192.34, "volume": 120, "ts": 1718539200000}
"""
from __future__ import annotations

import json
import os
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock_quotes")
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


def run_live(producer: KafkaProducer) -> None:
    """Vraies cotations via le websocket Finnhub."""
    import websocket  # websocket-client

    if not FINNHUB_KEY:
        raise RuntimeError("FINNHUB_API_KEY manquant. Renseignez-le dans .env.")

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


def main() -> None:
    producer = build_producer()
    run_live(producer)


if __name__ == "__main__":
    main()
