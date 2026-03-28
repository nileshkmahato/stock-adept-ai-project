{{
    config(
        materialized='view',
        tags=['yfin'],
        pre_hook="{{ create_yfin_external_table() }}",
        alias='stg_yfin_raw_data'
    )
}}

SELECT

    stock_symbol as ticker,
    data_type_label,
    SAFE_CAST(timestamp AS date) as trade_date,
    SAFE_CAST(Open AS FLOAT64) as open,
    SAFE_CAST(High AS FLOAT64) as high,
    SAFE_CAST(Low AS FLOAT64) as low,
    SAFE_CAST(Close AS FLOAT64) as close,
    SAFE_CAST(Volume AS INT64) as volume,
    timestamp as event_time 
FROM {{source('gfin_raw_data','yfin_stock_ticks_ext')}}
where close is not null