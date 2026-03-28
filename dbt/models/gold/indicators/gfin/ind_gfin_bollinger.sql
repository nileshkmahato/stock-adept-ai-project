{{
    config(materialized='view', tags=['gfin','indicator'])
}}

select
    event_time,
    trade_date,
    ticker,
    price,
    avg(price) over (partition by ticker order by event_time rows between 19 preceding and current row) as middle_band,
    avg(price) over (partition by ticker order by event_time rows between 19 preceding and current row)
        + 2 * stddev(price) over (partition by ticker order by event_time rows between 19 preceding and current row) as upper_band,
    avg(price) over (partition by ticker order by event_time rows between 19 preceding and current row)
        - 2 * stddev(price) over (partition by ticker order by event_time rows between 19 preceding and current row) as lower_band
from {{ ref('slv_gfin_ticks') }}
