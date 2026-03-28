{{
    config(materialized='view', tags=['yfin','indicator'])
}}

select
    trade_date,
    ticker,
    close,
    avg(close) over (
        partition by ticker
        order by trade_date
        rows between 19 preceding and current row
    ) as sma_20
from {{ ref('slv_yfin_daily') }}
