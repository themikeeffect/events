SELECT
  user_id,
  event_time,
  COUNT(*) OVER (PARTITION BY user_id ORDER BY event_time
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_number
FROM {{ source('Events', 'event_stream') }}