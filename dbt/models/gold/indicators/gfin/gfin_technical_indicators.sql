{{  
    config(materialized='view', tags=['gfin','indicator'])  
}}

WITH price_data AS (
    SELECT
        ticker,
        event_time,
        price,
        DATE(event_time) AS date,
        EXTRACT(HOUR FROM event_time) AS hour
    FROM {{ source('gfin_raw_data_clean','stg_gfin_raw_clean_data') }}
    WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
),

sma_calculations AS (
    SELECT
        ticker,
        event_time,
        price,
        date,
        hour,
        -- SMA for different windows
        AVG(price) OVER (
            PARTITION BY ticker
            ORDER BY event_time
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS sma_5,
        AVG(price) OVER (
            PARTITION BY ticker
            ORDER BY event_time
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sma_10,
        AVG(price) OVER (
            PARTITION BY ticker
            ORDER BY event_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sma_20,
        AVG(price) OVER (
            PARTITION BY ticker
            ORDER BY event_time
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50,
        AVG(price) OVER (
            PARTITION BY ticker
            ORDER BY event_time
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS sma_200
    FROM price_data
),

ema_calculations AS (
    SELECT
        *,
        -- EMA approximations using smoothing factors
        0.1538 * price + 0.8462 * LAG(price) OVER (PARTITION BY ticker ORDER BY event_time) AS ema_12,
        0.0741 * price + 0.9259 * LAG(price) OVER (PARTITION BY ticker ORDER BY event_time) AS ema_26
    FROM sma_calculations
),

rsi_calculation AS (
    SELECT
        *,
        CASE 
            WHEN avg_loss_14 = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain_14 / NULLIF(avg_loss_14, 0))))
        END AS rsi_14
    FROM (
        SELECT
            *,
            AVG(gain) OVER (
                PARTITION BY ticker
                ORDER BY event_time
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS avg_gain_14,
            AVG(loss) OVER (
                PARTITION BY ticker
                ORDER BY event_time
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS avg_loss_14
        FROM (
            SELECT
                *,
                GREATEST(price - LAG(price) OVER (PARTITION BY ticker ORDER BY event_time), 0) AS gain,
                ABS(LEAST(price - LAG(price) OVER (PARTITION BY ticker ORDER BY event_time), 0)) AS loss
            FROM ema_calculations
        )
    )
),

macd_calculation AS (
    SELECT
        *,
        ema_12 - ema_26 AS macd_line,
        -- Signal line: 9-period EMA of MACD line
        0.2 * (ema_12 - ema_26) + 0.8 * LAG(ema_12 - ema_26) OVER (PARTITION BY ticker ORDER BY event_time) AS signal_line_9,
        (ema_12 - ema_26) -
          (0.2 * (ema_12 - ema_26) + 0.8 * LAG(ema_12 - ema_26) OVER (PARTITION BY ticker ORDER BY event_time)) AS macd_histogram
    FROM rsi_calculation
),

bollinger_calculation AS (
    SELECT
        *,
        sma_20 AS bollinger_middle,
        sma_20 + (2 * std_dev_20) AS bollinger_upper,
        sma_20 - (2 * std_dev_20) AS bollinger_lower,
        (price - sma_20) / (2 * std_dev_20) AS bollinger_percent
    FROM (
        SELECT
            *,
            STDDEV(price) OVER (
                PARTITION BY ticker
                ORDER BY event_time
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS std_dev_20
        FROM macd_calculation
    )
),

volume_metrics AS (
    SELECT
        g.ticker,
        g.event_time,
        g.price,
        g.avg_volume,
        COUNT(*) OVER (
            PARTITION BY g.ticker, DATE(g.event_time)
            ORDER BY g.event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_ticks,
        AVG(g.price) OVER (
            PARTITION BY g.ticker
            ORDER BY g.event_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS volatility_sma_20,
        STDDEV(g.price) OVER (
            PARTITION BY g.ticker
            ORDER BY g.event_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS price_volatility_20
    FROM fit-land-343712.stock_data_raw__staging.stg_gfin_raw_data g
    WHERE g.event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
)

SELECT
    b.ticker,
    b.event_time,
    b.date,
    b.hour,
    b.price,
    -- SMA Values
    b.sma_5,
    b.sma_10,
    b.sma_20,
    b.sma_50,
    b.sma_200,
    -- EMA Values
    b.ema_12,
    b.ema_26,
    -- RSI
    b.rsi_14,
    -- MACD
    b.macd_line,
    b.signal_line_9 AS macd_signal_line,
    b.macd_histogram,
    -- Bollinger Bands
    b.bollinger_middle,
    b.bollinger_upper,
    b.bollinger_lower,
    b.bollinger_percent,
    -- Volume & Volatility
    v.avg_volume,
    v.cumulative_ticks,
    v.volatility_sma_20,
    v.price_volatility_20,
    -- Additional calculated fields
    b.price / b.sma_20 AS price_sma_20_ratio,
    (b.price - b.sma_20) / b.sma_20 * 100 AS price_vs_sma_20_pct,
    CASE 
        WHEN b.rsi_14 < 30 THEN 'OVERSOLD'
        WHEN b.rsi_14 > 70 THEN 'OVERBOUGHT'
        ELSE 'NEUTRAL'
    END AS rsi_signal,
    CASE 
        WHEN b.macd_line > b.signal_line_9 AND LAG(b.macd_line) OVER (PARTITION BY b.ticker ORDER BY b.event_time) <= LAG(b.signal_line_9) OVER (PARTITION BY b.ticker ORDER BY b.event_time)
            THEN 'BULLISH_CROSSOVER'
        WHEN b.macd_line < b.signal_line_9 AND LAG(b.macd_line) OVER (PARTITION BY b.ticker ORDER BY b.event_time) >= LAG(b.signal_line_9) OVER (PARTITION BY b.ticker ORDER BY b.event_time)
            THEN 'BEARISH_CROSSOVER'
        ELSE 'NO_CROSSOVER'
    END AS macd_signal,
    CURRENT_TIMESTAMP() AS calculated_at
FROM bollinger_calculation b
LEFT JOIN volume_metrics v 
  ON b.ticker = v.ticker AND b.event_time = v.event_time