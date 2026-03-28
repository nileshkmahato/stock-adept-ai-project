{{
    config(materialized='view', tags=['dashboard','gfin'])
}}

select
    g.event_time,
    g.ticker,
    g.price,
    s.sma_20,
    e.ema_12,
    r.rsi_14,
    b.upper_band,
    b.lower_band
from {{ ref('slv_gfin_ticks') }} g
left join {{ ref('ind_gfin_sma') }} s using (event_time, ticker)
left join {{ ref('ind_gfin_ema') }} e using (event_time, ticker)
left join {{ ref('ind_gfin_rsi') }} r using (event_time, ticker)
left join {{ ref('ind_gfin_bollinger') }} b using (event_time, ticker)
