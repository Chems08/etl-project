-- Couche ELT : transformations SQL appliquées PAR-DESSUS la couche core.
-- 3 vues (window functions) + 1 table analytique de reporting.
-- NB PostgreSQL : ROUND(x, n) exige un numeric -> casts ::numeric.

-- 1) Rendement quotidien (%) vs la veille.
CREATE OR REPLACE VIEW analytics.daily_returns AS
SELECT
    ticker,
    trade_date,
    close,
    LAG(close) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_close,
    ROUND(
        ((close / NULLIF(LAG(close) OVER (PARTITION BY ticker ORDER BY trade_date), 0) - 1) * 100)::numeric,
        3
    ) AS daily_return_pct
FROM core.stock_prices;

-- 2) Moyennes mobiles 7 j / 30 j du cours de clôture.
CREATE OR REPLACE VIEW analytics.moving_averages AS
SELECT
    ticker,
    trade_date,
    close,
    ROUND((AVG(close) OVER (
        PARTITION BY ticker ORDER BY trade_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW))::numeric, 2) AS ma_7,
    ROUND((AVG(close) OVER (
        PARTITION BY ticker ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))::numeric, 2) AS ma_30
FROM core.stock_prices;

-- 3) Volatilité : écart-type glissant (30 j) des rendements.
CREATE OR REPLACE VIEW analytics.volatility AS
SELECT
    ticker,
    trade_date,
    daily_return_pct,
    ROUND((STDDEV_SAMP(daily_return_pct) OVER (
        PARTITION BY ticker ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))::numeric, 3) AS volatility_30d
FROM analytics.daily_returns;

-- 4) Table analytique de reporting : une ligne par ticker, reconstruite à chaque run.
DROP TABLE IF EXISTS analytics.ticker_summary;
CREATE TABLE analytics.ticker_summary AS
WITH bounds AS (
    SELECT ticker,
           MIN(trade_date) AS first_date,
           MAX(trade_date) AS last_date,
           AVG(volume)     AS avg_volume,
           MAX(high)       AS period_high,
           MIN(low)        AS period_low
    FROM core.stock_prices
    GROUP BY ticker
),
first_last AS (
    SELECT b.ticker, b.avg_volume, b.period_high, b.period_low, b.last_date,
           fp.close AS first_close, lp.close AS last_close
    FROM bounds b
    JOIN core.stock_prices fp ON fp.ticker = b.ticker AND fp.trade_date = b.first_date
    JOIN core.stock_prices lp ON lp.ticker = b.ticker AND lp.trade_date = b.last_date
),
latest_vol AS (
    SELECT DISTINCT ON (ticker) ticker, volatility_30d, daily_return_pct
    FROM analytics.volatility
    ORDER BY ticker, trade_date DESC
)
SELECT
    fl.ticker,
    ROUND(fl.last_close::numeric, 2)                                            AS last_close,
    ROUND((((fl.last_close / NULLIF(fl.first_close, 0)) - 1) * 100)::numeric, 2) AS period_return_pct,
    lv.daily_return_pct                                                         AS last_daily_return_pct,
    lv.volatility_30d,
    ROUND(fl.period_high::numeric, 2)                                           AS period_high,
    ROUND(fl.period_low::numeric, 2)                                            AS period_low,
    ROUND(fl.avg_volume::numeric, 0)                                            AS avg_volume,
    fl.last_date,
    now()                                                                       AS refreshed_at
FROM first_last fl
LEFT JOIN latest_vol lv ON lv.ticker = fl.ticker
ORDER BY period_return_pct DESC;
