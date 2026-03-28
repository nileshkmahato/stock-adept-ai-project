{{
    config(materialized='view', tags=['yfin','indicator'])
}}

with deltas as (
    select *,
        close - lag(close) over (partition by ticker order by trade_date) as delta
    from {{ ref('slv_yfin_daily') }}
)

select
    trade_date,
    ticker,
    100 - (
        100 / (
            1 + avg(greatest(delta,0))
            over (partition by ticker order by trade_date rows between 13 preceding and current row)
        )
    ) as rsi_14
from deltas
