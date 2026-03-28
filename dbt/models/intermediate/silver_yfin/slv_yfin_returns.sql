{{
    config(materialized='view', tags=['yfin','silver'])
}}

select
    *,
    ((close - prev_close) / prev_close) * 100 as daily_return_pct
from {{ ref('slv_yfin_daily') }}
where prev_close is not null
