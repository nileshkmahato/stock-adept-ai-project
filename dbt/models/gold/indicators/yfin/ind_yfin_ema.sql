{{
    config(materialized='view', tags=['yfin','indicator'])
}}

select
    trade_date,
    ticker,
    close,
    avg(close) over (partition by ticker order by trade_date rows between 11 preceding and current row) as ema_12,
    avg(close) over (partition by ticker order by trade_date rows between 25 preceding and current row) as ema_26
from {{ ref('slv_yfin_daily') }}
