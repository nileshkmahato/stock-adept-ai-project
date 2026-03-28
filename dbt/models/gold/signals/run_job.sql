{{
    config(materialized='table', tags=['signal'])
}}

SELECT * FROM {{ref("daily_alert_job")}}