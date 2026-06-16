---
marp: true
theme: uncover
class: invert
paginate: true
---

<!--
Gabarit de présentation (5–7 min). Couvre les 5 points exigés par le cahier des charges.
Rendu en slides :
  - VS Code : extension "Marp for VS Code" → export PDF/PPTX
  - ou CLI  : npx @marp-team/marp-cli slides.md --pdf
À personnaliser : captures d'écran, chiffres réels de votre run, anecdotes de démo.
-->

# 📈 STOCKDESK
## Pipeline d'analyse boursière en temps réel

ETL & Pipeline Orchestration — ESILV MSc A4
**Chems MITTA**

---

## 1 · Cas d'usage

**Problème** : suivre un panier d'actions (AAPL, MSFT, TSLA, AMZN, GOOGL, NVDA, META, JPM)
au même endroit — historique, indicateurs analytiques et cotations live.

**Données** :
- Cours historiques journaliers → `yfinance`
- Cotations temps réel → API `Finnhub` (WebSocket)

**Valeur** : un seul tableau de bord pour décider, alimenté par un pipeline automatisé.

---

## 2 · Architecture

```
yfinance ─► ETL batch ─► staging ─► core ─► analytics ─┐
                                                       ├─► Dashboard
Finnhub ─► Kafka producer ─► topic ─► consumer ─► realtime ─┘
       Airflow orchestre le batch + les transformations SQL
```

> (Insérer ici le diagramme `architecture.md` exporté en PNG.)

Stack : Python · PostgreSQL · Kafka · Airflow · SQL · Streamlit · Docker

---

## 3 · Démo en direct — le pipeline

- **ETL batch** : `yfinance` → nettoyage → `core.stock_prices` (**idempotent**, `ON CONFLICT`)
- **ELT SQL** : `staging` → vues `analytics.*` (rendements, MA 7/30j, volatilité) → table `ticker_summary`
- **Orchestration** : DAG Airflow `ensure_schema → ingest_batch → transform`
  (planifié `@daily`, 2 retries, dépendances)

*(Montrer le DAG vert dans Airflow + relancer pour prouver l'idempotence.)*

---

## 4 · Démo en direct — le streaming + dashboard

- **Producer** → topic Kafka `stock_quotes` (vraies cotations Finnhub)
- **Consumer** → calcule variation + bougies 1 min → `realtime.*`
- **Dashboard Streamlit** (5 visualisations, rafraîchi 5 s) :
  ① chandelier ② moyennes mobiles ③ rendements + volatilité
  ④ tableau de reporting ⑤ cotations live + intraday

*(Ouvrir le dashboard, montrer les prix qui bougent.)*

---

## 5 · Flux de données — récapitulatif

| Étape | Outil | Sortie |
|-------|-------|--------|
| Ingestion batch | yfinance | `staging` → `core` |
| Transformation | SQL (window functions) | `analytics.*` |
| Streaming | Kafka producer/consumer | `realtime.*` |
| Orchestration | Airflow DAG | exécutions planifiées |
| Visualisation | Streamlit + Plotly | dashboard live |

---

# Merci !
### Questions ?

Code + README + SETUP + diagramme → soumis via Teams
