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
        rows between 19 preceding and current row
    ) as sma_20
from {{ ref('slv_gfin_ticks') }}
