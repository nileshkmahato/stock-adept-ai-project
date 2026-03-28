{{ config(
    materialized='view', 
    tags=['gfin'],
    alias='stg_gfin_raw_clean_data'
) }}

WITH cleaned_data AS (
    SELECT
        ticker,
        exchange,
        event_time,
        
        -- Clean price field (remove currency symbol and commas)
        SAFE_CAST(REGEXP_REPLACE(CAST(price AS STRING), '[₹$,]', '') AS FLOAT64) AS price,
        SAFE_CAST(REGEXP_REPLACE(CAST(previous_close AS STRING), '[₹$,]', '') AS FLOAT64) AS previous_close,
        
        -- Parse day range safely
        SAFE_CAST(SPLIT(REGEXP_REPLACE(CAST(day_range AS STRING), '[₹$,]', ''), ' - ')[SAFE_OFFSET(0)] AS FLOAT64) AS day_low,
        SAFE_CAST(SPLIT(REGEXP_REPLACE(CAST(day_range AS STRING), '[₹$,]', ''), ' - ')[SAFE_OFFSET(1)] AS FLOAT64) AS day_high,
        
        -- Parse year range safely
        SAFE_CAST(SPLIT(REGEXP_REPLACE(CAST(year_range AS STRING), '[₹$,]', ''), ' - ')[SAFE_OFFSET(0)] AS FLOAT64) AS year_low,
        SAFE_CAST(SPLIT(REGEXP_REPLACE(CAST(year_range AS STRING), '[₹$,]', ''), ' - ')[SAFE_OFFSET(1)] AS FLOAT64) AS year_high,
        
        -- Clean market cap
        CASE 
            WHEN REGEXP_CONTAINS(CAST(market_cap AS STRING), 'T$') 
                THEN SAFE_CAST(REGEXP_REPLACE(CAST(market_cap AS STRING), '[A-Za-z₹$, ]', '') AS FLOAT64) * 1e12
            WHEN REGEXP_CONTAINS(CAST(market_cap AS STRING), 'B$') 
                THEN SAFE_CAST(REGEXP_REPLACE(CAST(market_cap AS STRING), '[A-Za-z₹$, ]', '') AS FLOAT64) * 1e9
            ELSE SAFE_CAST(REGEXP_REPLACE(CAST(market_cap AS STRING), '[A-Za-z₹$, ]', '') AS FLOAT64)
        END AS market_cap_clean,
        market_cap,
        
        -- Clean volume (remove M suffix, commas, currency symbols)
        SAFE_CAST(REGEXP_REPLACE(CAST(avg_volume AS STRING), '[A-Za-z₹$,]', '') AS FLOAT64) * 1e6 AS avg_volume_clean,
        avg_volume,
        
        SAFE_CAST(CAST(pe_ratio AS STRING) AS FLOAT64) AS pe_ratio,
        ingestion_time
    FROM {{ref('stg_gfin_raw_data')}}
)

SELECT
    ticker,
    exchange,
    event_time,
    price,
    previous_close,
    day_low,
    day_high,
    year_low,
    year_high,
    market_cap_clean,market_cap,
    avg_volume_clean,avg_volume,
    pe_ratio,
    ingestion_time,
    DATE(event_time) AS date,
    FORMAT_TIMESTAMP('%I:%M:%S %p', ingestion_time, 'Asia/Kolkata') AS ist_time_12hr_format

FROM cleaned_data
