SELECT
  event_time,
  user_id,
  event_type,
  transaction_category,
  miles_amount
FROM {{ ref('Events.event_stream') }};