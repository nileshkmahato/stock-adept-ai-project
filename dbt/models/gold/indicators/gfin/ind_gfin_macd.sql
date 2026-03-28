{{
    config(materialized='view', tags=['gfin','indicator'])
}}

with ema as (
    select
        event_time,
        trade_date,
        ticker,
        avg(price) over (partition by ticker order by event_time rows between 11 preceding and current row) as ema_12,
        avg(price) over (partition by ticker order by event_time rows between 25 preceding and current row) as ema_26
    from {{ ref('slv_gfin_ticks') }}
)

select
    *,
    ema_12 - ema_26 as macd_line,
    avg(ema_12 - ema_26) over (
        partition by ticker
        order by event_time
        rows between 8 preceding and current row
    ) as macd_signal
from ema
