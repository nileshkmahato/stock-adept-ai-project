{{
    config(materialized='view', tags=['dashboard','yfin'])
}}

select
    d.trade_date,
    d.ticker,
    d.close,
    s.sma_20,
    e.ema_12,
    r.rsi_14,
    m.macd_line,
    m.macd_signal,
    b.upper_band,
    b.lower_band
from {{ ref('slv_yfin_daily') }} d
left join {{ ref('ind_yfin_sma') }} s using (trade_date, ticker)
left join {{ ref('ind_yfin_ema') }} e using (trade_date, ticker)
left join {{ ref('ind_yfin_rsi') }} r using (trade_date, ticker)
left join {{ ref('ind_yfin_macd') }} m using (trade_date, ticker)
left join {{ ref('ind_yfin_bollinger') }} b using (trade_date, ticker)
