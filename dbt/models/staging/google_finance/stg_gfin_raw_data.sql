{{
    config(
        materialized='view',
        tags=['gfin'],
        alias='stg_gfin_raw_data'
    )
}}

SELECT *,DATE(ingestion_time) AS date FROM {{source('gfin_raw_data','gfin_stock_ticks_ext')}}
where price is not null AND REGEXP_CONTAINS(CAST(price AS STRING), r'^\d')