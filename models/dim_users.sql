WITH first_event AS (
  SELECT
    user_id,
    MIN(event_time) AS first_event_time
  FROM {{ ref('event_stream') }}
  GROUP BY user_id
),
user_details AS (
  SELECT
    e.user_id,
    e.event_time AS first_event_time,
    e.platform AS first_platform,
    e.utm_source AS first_utm_source,
    e.country AS first_country
  FROM {{ ref('event_stream') }} e
  JOIN first_event f
    ON e.user_id = f.user_id AND e.event_time = f.first_event_time
)

SELECT * FROM user_details;