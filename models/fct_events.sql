SELECT
  event_time,
  user_id,
  event_type,
  transaction_category,
  miles_amount
FROM {{ source('Events', 'event_stream') }}