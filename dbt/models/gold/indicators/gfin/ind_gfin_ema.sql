{{
    config(materialized='view', tags=['gfin','indicator'])
}}

select
    event_time,
    trade_date,
    ticker,
    price,
    avg(price) over (
        partition by ticker
        order by event_time
        rows between 11 preceding and current row 
    ) as ema_12,
    avg(price) over (
        partition by ticker
        order by event_time
        rows between 25 preceding and current row
    ) as ema_26
from {{ ref('slv_gfin_ticks') }}
