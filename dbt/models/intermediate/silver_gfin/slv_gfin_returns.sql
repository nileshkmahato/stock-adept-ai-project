{{
    config(materialized='view',
    tags=['gfin','silver'])
}}

select
    *,
    ((price - previous_close) / previous_close) * 100 as price_change_pct
from {{ source('gfin_raw_data_clean','stg_gfin_raw_clean_data') }}
where previous_close is not null
