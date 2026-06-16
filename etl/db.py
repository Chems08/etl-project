"""Helpers de connexion PostgreSQL partagés (ETL, consumer, dashboard, DAG).

Toutes les coordonnées de connexion proviennent des variables d'environnement
définies dans docker-compose / .env, pour qu'aucun secret ne soit en dur.
"""
from __future__ import annotations

import os
import time

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def _params() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "stockdb"),
        "user": os.getenv("POSTGRES_USER", "stock"),
        "password": os.getenv("POSTGRES_PASSWORD", "stock"),
    }


def get_conn(retries: int = 10, delay: int = 3) -> "psycopg2.extensions.connection":
    """Connexion psycopg2, avec quelques tentatives (le temps que Postgres démarre)."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(**_params())
        except psycopg2.OperationalError as err:  # base pas encore prête
            last_err = err
            print(f"[db] Postgres indisponible (tentative {attempt}/{retries})…")
            time.sleep(delay)
    raise RuntimeError(f"Impossible de joindre Postgres: {last_err}")


def get_engine() -> Engine:
    """Engine SQLAlchemy (utilisé par pandas to_sql / read_sql)."""
    p = _params()
    url = f"postgresql+psycopg2://{p['user']}:{p['password']}@{p['host']}:{p['port']}/{p['dbname']}"
    return create_engine(url, pool_pre_ping=True)


def run_sql_file(path: str) -> None:
    """Exécute un fichier .sql complet (DDL ou transformations)."""
    with open(path, "r", encoding="utf-8") as fh:
        sql = fh.read()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print(f"[db] SQL exécuté : {path}")
    finally:
        conn.close()
