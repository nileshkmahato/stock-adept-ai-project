{{
    config(materialized='view', tags=['yfin','silver'])
}}

select
    trade_date,
    ticker,
    stddev(daily_return_pct) over (
        partition by ticker
        order by trade_date
        rows between 19 preceding and current row
    ) as volatility_20d
from {{ ref('slv_yfin_returns') }}
