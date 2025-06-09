{{
  config(
    materialized='incremental',
    unique_key = 'user_id',
    incremental_strategy = 'merge'
  )
}}

SELECT
  event_time,
  user_id,
  event_type,
  transaction_category,
  miles_amount
FROM {{ source('Events', 'event_stream') }}