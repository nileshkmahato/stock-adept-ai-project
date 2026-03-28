{{
    config(materialized='view', tags=['gfin','indicator'])
}}

with deltas as (
    select *,
        price - lag(price) over (partition by ticker order by event_time) as delta
    from {{ ref('slv_gfin_ticks') }}
)

select
    event_time,
    trade_date,
    ticker,
    100 - (
        100 / (
            1 + avg(greatest(delta,0))
            over (partition by ticker order by event_time rows between 13 preceding and current row)
        )
    ) as rsi_14
from deltas
