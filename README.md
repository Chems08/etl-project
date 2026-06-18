# 📈 STOCKDESK — Pipeline d'analyse boursière en temps réel

Projet final — *ETL & Pipeline Orchestration* · ESILV MSc A4 (MACSIN4A2125)
Auteur : **Chems MITTA**

## Cas d'usage

Un investisseur veut suivre un panier d'actions (Apple, Microsoft, Tesla, Amazon, Google, Nvidia,
Meta, JP Morgan) avec, au même endroit : l'**historique des cours**, des **indicateurs analytiques**
(rendements, moyennes mobiles, volatilité) et un **flux de cotations en direct**.

Ce projet implémente un **pipeline de données complet de bout en bout** qui ingère, transforme,
diffuse en streaming, orchestre et visualise ces données boursières.

## Ce que fait le pipeline

| # | Composant | Description | Fichiers |
|---|-----------|-------------|----------|
| 1 | **ETL Batch** | Ingestion des cours historiques via `yfinance`, nettoyage (cast, dédup), chargement **idempotent** dans PostgreSQL | `etl/batch_ingest.py` |
| 2 | **ELT (SQL)** | Couche staging → transformations SQL (vues + table de reporting) : rendements, moyennes mobiles, volatilité | `sql/*.sql` |
| 3 | **Streaming** | Cotations live (Finnhub) → topic **Kafka** → traitement **Spark Structured Streaming** (OHLC 1 min) → sink PostgreSQL | `streaming/producer.py`, `streaming/spark_consumer.py` |
| 4 | **Orchestration** | DAG **Apache Airflow** : planification quotidienne, retries, dépendances entre tâches | `dags/stock_pipeline_dag.py` |
| 5 | **Dashboard** | **Streamlit + Plotly**, 5 visualisations rafraîchies en continu | `dashboard/app.py` |

## Architecture

Voir [architecture.md](architecture.md) pour le diagramme de flux complet.

```
yfinance ─► ETL batch ─► staging ─► core ─► analytics ─┐
                                                       ├─► Dashboard Streamlit
Finnhub ─► Kafka producer ─► topic ─► Spark Streaming ─► realtime ─┘
                 Airflow orchestre l'ETL batch + les transformations SQL
```

## Les 5 visualisations du dashboard

1. **Chandelier** des cours journaliers (depuis `core.stock_prices`).
2. **Moyennes mobiles** 7j / 30j superposées au cours.
3. **Rendements quotidiens** (barres) + **volatilité** 30j (ligne).
4. **Tableau de reporting** + classement des performances (depuis `analytics.ticker_summary`).
5. **Cotations temps réel** : KPIs live, bougies intraday 1 min, tableau des derniers prix.

## Stack technique

`Python` · `yfinance` · `PostgreSQL` · `Apache Kafka` (KRaft) · `Apache Spark` (Structured Streaming) ·
`Apache Airflow` · `SQL` · `Streamlit` · `Plotly` · `Docker Compose`

## Idempotence

L'ETL batch s'appuie sur la clé naturelle `(ticker, trade_date)` et un `INSERT … ON CONFLICT DO
UPDATE` : **relancer le pipeline ne crée jamais de doublon**.

## Démarrage rapide

```bash
cp .env.example .env        # puis renseigner FINNHUB_API_KEY
docker compose up -d --build
```

- Airflow : <http://localhost:8080> (admin / admin) → déclencher le DAG `stock_pipeline`
- Dashboard : <http://localhost:8501>

Instructions détaillées dans [SETUP.md](SETUP.md).

## Source de données

- **Batch** : [yfinance](https://pypi.org/project/yfinance/) (Yahoo Finance), gratuit, sans clé.
- **Streaming** : [Finnhub](https://finnhub.io) WebSocket (clé gratuite).

## Évolutions possibles

- Remplacer les vues SQL par des **modèles dbt** (tests + lineage).
- Passer Spark d'un master `local[2]` à un vrai **cluster Spark** (master + workers) pour scaler.
