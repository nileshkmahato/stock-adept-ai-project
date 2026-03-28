{{
    config(
        materialized='view', 
        tags=['gfin','silver']
    )
}}

select
    event_time,
    date as trade_date,
    ticker,
    price,
    previous_close
FROM {{ source('gfin_raw_data_clean','stg_gfin_raw_clean_data') }}
