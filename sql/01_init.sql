-- Initialisation du schéma (DDL). Exécuté par le DAG (tâche ensure_schema) et par
-- le job Spark streaming au démarrage — idempotent (IF NOT EXISTS).
--
-- Couches : staging (brut) · core (vérité) · analytics (reporting) · realtime (stream)
-- NB : la table staging.stock_prices_raw est (re)créée automatiquement par pandas
--      (to_sql) lors de l'ingestion ; seul son schéma est déclaré ici.

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS realtime;

-- Source de vérité : clé naturelle (ticker, trade_date) -> upsert idempotent.
CREATE TABLE IF NOT EXISTS core.stock_prices (
    ticker      TEXT        NOT NULL,
    trade_date  DATE        NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION NOT NULL,
    adj_close   DOUBLE PRECISION,
    volume      BIGINT,
    ingested_at TIMESTAMP   DEFAULT now(),
    PRIMARY KEY (ticker, trade_date)
);

-- Sink du streaming : dernier tick par ticker (upsert sur la clé ticker).
CREATE TABLE IF NOT EXISTS realtime.live_quotes (
    ticker      TEXT PRIMARY KEY,
    price       DOUBLE PRECISION,
    prev_price  DOUBLE PRECISION,
    change_pct  DOUBLE PRECISION,
    volume      BIGINT,
    quote_ts    TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT now()
);

-- Sink du streaming : bougies 1 minute agrégées en continu.
CREATE TABLE IF NOT EXISTS realtime.ohlc_1min (
    ticker     TEXT        NOT NULL,
    minute_ts  TIMESTAMP   NOT NULL,
    open       DOUBLE PRECISION,
    high       DOUBLE PRECISION,
    low        DOUBLE PRECISION,
    close      DOUBLE PRECISION,
    tick_count INTEGER,
    PRIMARY KEY (ticker, minute_ts)
);
