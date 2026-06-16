"""Composant 4 — Orchestration (Apache Airflow).

DAG `stock_pipeline` : coordonne le pipeline batch + les transformations SQL.

  ensure_schema -> ingest_batch -> transform

Planification quotidienne, 2 retentatives par tâche, dépendances explicites.
Le streaming (producer/consumer) tourne en service continu, hors de ce DAG.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Code du pipeline, monté dans le conteneur Airflow (PYTHONPATH=/opt/airflow).
from etl.batch_ingest import run as run_batch
from etl.db import run_sql_file

SQL_DIR = "/opt/airflow/sql"


def ensure_schema() -> None:
    """Crée schémas + tables (core, realtime) avant le chargement."""
    run_sql_file(os.path.join(SQL_DIR, "01_init.sql"))


def transform() -> None:
    """Couche ELT : vues analytiques + table de reporting ticker_summary."""
    run_sql_file(os.path.join(SQL_DIR, "02_transforms.sql"))


default_args = {
    "owner": "chems",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_pipeline",
    description="Pipeline boursier : ingestion batch + transformations SQL",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["finance", "etl", "elt"],
) as dag:

    t_schema = PythonOperator(
        task_id="ensure_schema",
        python_callable=ensure_schema,
    )

    t_ingest = PythonOperator(
        task_id="ingest_batch",
        python_callable=run_batch,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t_schema >> t_ingest >> t_transform
