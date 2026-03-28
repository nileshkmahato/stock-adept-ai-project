{{
    config(materialized='view', tags=['yfin','indicator'])
}}

select
    trade_date,
    ticker,
    close,
    avg(close) over (partition by ticker order by trade_date rows between 19 preceding and current row) as middle_band,
    avg(close) over (partition by ticker order by trade_date rows between 19 preceding and current row)
        + 2 * stddev(close) over (partition by ticker order by trade_date rows between 19 preceding and current row) as upper_band,
    avg(close) over (partition by ticker order by trade_date rows between 19 preceding and current row)
        - 2 * stddev(close) over (partition by ticker order by trade_date rows between 19 preceding and current row) as lower_band
from {{ ref('slv_yfin_daily') }}
