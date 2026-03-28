{{
    config(materialized='view', tags=['yfin','indicator'])
}}

with ema as (
    select
        trade_date,
        ticker,
        avg(close) over (partition by ticker order by trade_date rows between 11 preceding and current row) as ema_12,
        avg(close) over (partition by ticker order by trade_date rows between 25 preceding and current row) as ema_26
    from {{ ref('slv_yfin_daily') }}
)

select
    *,
    ema_12 - ema_26 as macd_line,
    avg(ema_12 - ema_26) over (
        partition by ticker
        order by trade_date
        rows between 8 preceding and current row
    ) as macd_signal
from ema
