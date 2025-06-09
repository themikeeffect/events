WITH first_event_time AS (
  SELECT
    user_id,
    MIN(event_time) AS first_event_time
  FROM {{ source('Events', 'event_stream') }}
  GROUP BY user_id
),

first_event_details AS (
  SELECT
    e.user_id,
    e.event_time,
    e.platform AS first_platform,
    e.utm_source AS first_utm_source,
    e.country AS first_country
  FROM {{ source('Events', 'event_stream') }} e
  JOIN first_event_time fe
    ON e.user_id = fe.user_id
    AND e.event_time = fe.first_event_time
)

SELECT
  user_id,
  event_time AS first_event_time,
  first_platform,
  first_utm_source,
  first_country
FROM first_event_details