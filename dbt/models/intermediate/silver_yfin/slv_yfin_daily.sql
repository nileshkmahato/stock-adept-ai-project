{{
    config(materialized='view', tags=['yfin','silver'])
}}

select
    trade_date,
    ticker,
    close,
    volume,
    lag(close) over (partition by ticker,data_type_label order by trade_date) as prev_close
from {{ ref('stg_yfin_raw_data') }}
