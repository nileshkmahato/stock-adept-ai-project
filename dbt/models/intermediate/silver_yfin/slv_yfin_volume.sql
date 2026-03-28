{{
    config(materialized='view', tags=['yfin','silver'])
}}

select
    trade_date,
    ticker,
    volume,
    avg(volume) over (
        partition by ticker
        order by trade_date
        rows between 19 preceding and current row
    ) as avg_volume_20d
from {{ ref('slv_yfin_daily') }} 
