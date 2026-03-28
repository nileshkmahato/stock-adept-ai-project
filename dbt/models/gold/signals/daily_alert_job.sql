{{ config(materialized='view', tags=['signal']) }}

-- --------------------------------------------------
-- STOCK SIGNALS PIPELINE -- DBT Job for BigQuery with Connector
-- --------------------------------------------------
-- This dbt model generates BUY/HOLD recommendations by combining:
-- 1. Daily stock recommendations (latest per symbol).
-- 2. Latest price feed from a raw data source of GFIN(fiance).
-- It applies business rules to determine signals, restricts execution
-- to market hours, and triggers an email notification when a BUY signal is detected.
-- NOTE: All sensitive identifiers (project IDs, emails) have been replaced with generic placeholders.
-- --------------------------------------------------

-- Main query for Scheduled Execution
WITH recm AS (
  SELECT
    current_price,
    symbol,
    company_name,
    entry_low,
    entry_high,
    timestamp,
    confidence,
    action,
    news_summary,
    ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY timestamp DESC) AS rn
  FROM {{ source('stock_data_raw','daily_recommendations') }}
  WHERE DATE(timestamp) = CURRENT_DATE()
),

gfin AS (
  SELECT
    ticker,
    price,
    event_time,
    ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY event_time DESC) AS rn
  FROM {{ source('gfin_raw_data_clean','stg_gfin_raw_clean_data') }}
  WHERE DATE(event_time) = CURRENT_DATE()
),

signals AS (
  SELECT
    r.symbol,
    r.company_name,
    g.price AS latest_price,
    CONCAT(ROUND(r.entry_low, 2), '-', ROUND(r.entry_high, 2)) AS entry_range,
    CASE
      WHEN 
        g.price < r.current_price AND r.confidence='HIGH' AND r.action='BUY' AND
        r.entry_low <> r.entry_high AND 
        g.price BETWEEN r.entry_low AND r.entry_high 
      THEN 'BUY'
      ELSE 'HOLD'
    END AS recommendation,
    r.action AS news_action,
    r.confidence AS news_confidence,
    r.news_summary AS news_summary
  FROM recm r
  JOIN gfin g ON r.symbol = g.ticker
  WHERE r.rn = 1 AND g.rn = 1
  -- Restrict to NSE Timings (9:15 AM to 3:30 PM IST)
  AND EXTRACT(TIME FROM CURRENT_TIMESTAMP() AT TIME ZONE "Asia/Kolkata") 
      BETWEEN '09:15:00' AND '15:30:00'
)

SELECT 
  *,
  CASE 
    WHEN recommendation = 'BUY' THEN 
      {{ source('stock_data_raw','trigger_mail_notification') }}(
        JSON_OBJECT(
          "to_mail",      "alerts@example.com",   -- generic placeholder [Sending Multi-recipient Emails "a@mail.com,b.@mail.com"]
          "subject",      FORMAT("STOCK ALERT: %s BUY SIGNAL", symbol),
          "content_type", "html",
          "content",      FORMAT("""
            <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
              <h2 style="color: #2e7d32;">📈 Stock Buy Alert</h2>
              <p>The stock <b>%s (%s)</b> has triggered a <b>BUY</b> signal.</p>
              <table border="1" cellpadding="8" cellspacing="0" 
                     style="border-collapse: collapse; width: 100%%;">
                <tr style="background-color: #f5f5f5;">
                  <td><b>Current Price</b></td>
                  <td>₹%.2f</td>
                </tr>
                <tr>
                  <td><b>Entry Range</b></td>
                  <td>%s</td>
                </tr>
                <tr style="background-color: #f5f5f5;">
                  <td><b>News Action</b></td>
                  <td>%s</td>
                </tr>
                <tr>
                  <td><b>Confidence</b></td>
                  <td>%s</td>
                </tr>
                <tr style="background-color: #f5f5f5;">
                  <td><b>News Summary</b></td>
                  <td>%s</td>
                </tr>
              </table>
              <br>
              <p style="color: #888; font-size: 12px;">
                <i>Automated alert from BigQuery Pipeline.</i>
              </p>
            </body>
            </html>
          """,
          symbol, company_name, latest_price, entry_range,
          news_action, CAST(news_confidence AS STRING), news_summary
          )
        )
      )
    ELSE 'HOLD - No mail sent'
  END AS execution_status
FROM signals;