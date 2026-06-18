"""Composant 5 — Dashboard temps réel (Streamlit).

Version simple : 5 visualisations lues depuis la sortie du pipeline, rafraîchies
automatiquement toutes les 5 s. On privilégie les graphiques natifs Streamlit
(st.line_chart / st.bar_chart) ; seul le chandelier utilise Plotly.

  1. Chandelier des cours            (core.stock_prices)
  2. Moyennes mobiles 7j / 30j       (analytics.moving_averages)
  3. Rendements quotidiens           (analytics.daily_returns)
  4. Performance par action          (analytics.ticker_summary)
  5. Cotations temps réel + intraday (realtime.live_quotes / ohlc_1min)
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# Rendre le package etl importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl.db import get_engine  # noqa: E402

st.set_page_config(page_title="Pipeline Boursier", page_icon="📈", layout="wide")
ENGINE = get_engine()


@st.cache_data(ttl=30)
def load(sql: str) -> pd.DataFrame:
    """Lecture (avec petit cache) des tables batch/analytics."""
    try:
        return pd.read_sql(sql, ENGINE)
    except Exception:
        return pd.DataFrame()


def load_live(sql: str) -> pd.DataFrame:
    """Lecture sans cache pour le flux temps réel."""
    try:
        return pd.read_sql(sql, ENGINE)
    except Exception:
        return pd.DataFrame()


# Rafraîchissement automatique toutes les 5 secondes.
st_autorefresh(interval=5000, key="refresh")

st.title("📈 Pipeline d'analyse boursière en temps réel")
st.caption("ETL · ELT · Kafka · Airflow · Streamlit — données rafraîchies toutes les 5 s")

tickers = load("SELECT DISTINCT ticker FROM core.stock_prices ORDER BY ticker")
if tickers.empty:
    st.info("Aucune donnée pour le moment. Lancez le DAG `stock_pipeline` dans Airflow "
            "(http://localhost:8080), puis rafraîchissez.")
    st.stop()

ticker = st.selectbox("Action", tickers["ticker"])

# ─── Bandeau cotations live (KPIs) ───────────────────────────────────────────
live = load_live("SELECT ticker, price, change_pct FROM realtime.live_quotes ORDER BY ticker")
if not live.empty:
    for col, (_, row) in zip(st.columns(len(live)), live.iterrows()):
        col.metric(row["ticker"], f"{row['price']:.2f}", f"{row['change_pct']:+.2f}%")

st.divider()

# ─── 1. Chandelier des cours (Plotly) ────────────────────────────────────────
st.subheader(f"1 · Cours journaliers — {ticker}")
px = load(f"SELECT trade_date, open, high, low, close FROM core.stock_prices "
          f"WHERE ticker = '{ticker}' ORDER BY trade_date")
if not px.empty:
    fig = go.Figure(go.Candlestick(
        x=px["trade_date"], open=px["open"], high=px["high"],
        low=px["low"], close=px["close"]))
    fig.update_layout(height=350, margin=dict(l=0, r=0, t=10, b=0),
                      xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)

c1, c2 = st.columns(2)

# ─── 2. Moyennes mobiles (graphe natif) ──────────────────────────────────────
with c1:
    st.subheader("2 · Moyennes mobiles 7j / 30j")
    ma = load(f"SELECT trade_date, close, ma_7, ma_30 FROM analytics.moving_averages "
              f"WHERE ticker = '{ticker}' ORDER BY trade_date")
    if not ma.empty:
        st.line_chart(ma.set_index("trade_date"))

# ─── 3. Rendements quotidiens (graphe natif) ─────────────────────────────────
with c2:
    st.subheader("3 · Rendements quotidiens (%)")
    ret = load(f"SELECT trade_date, daily_return_pct FROM analytics.daily_returns "
               f"WHERE ticker = '{ticker}' ORDER BY trade_date")
    if not ret.empty:
        st.bar_chart(ret.set_index("trade_date"))

# ─── 4. Performance par action (table de reporting) ──────────────────────────
st.subheader("4 · Performance par action")
summary = load("SELECT ticker, last_close, period_return_pct, volatility_30d, avg_volume "
               "FROM analytics.ticker_summary ORDER BY period_return_pct DESC")
if not summary.empty:
    c3, c4 = st.columns([2, 3])
    c3.bar_chart(summary.set_index("ticker")["period_return_pct"])
    c4.dataframe(summary, use_container_width=True, hide_index=True)

# ─── 5. Cotations temps réel (intraday) ──────────────────────────────────────
st.subheader(f"5 · Cotations temps réel — {ticker}")
intraday = load_live(f"SELECT minute_ts, close FROM realtime.ohlc_1min "
                     f"WHERE ticker = '{ticker}' ORDER BY minute_ts DESC LIMIT 60")
if not intraday.empty:
    st.line_chart(intraday.sort_values("minute_ts").set_index("minute_ts"))
else:
    st.caption("En attente du flux Kafka (producer + Spark)…")
