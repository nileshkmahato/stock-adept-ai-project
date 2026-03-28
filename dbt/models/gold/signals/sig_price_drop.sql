{{
    config(materialized='view', tags=['signal'])
}}

select
    trade_date,
    ticker,
    daily_return_pct,
    case
        when daily_return_pct <= -3 then 'HIGH_DROP'
        when daily_return_pct <= -2 then 'DROP'
    end as signal_type
from {{ ref('slv_yfin_returns') }}
where daily_return_pct <= -2
