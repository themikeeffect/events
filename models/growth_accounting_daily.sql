WITH all_events AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(event_time), DAY) AS day,
    MIN(event_time) OVER (PARTITION BY user_id) AS first_seen
  FROM {{ source('Events', 'event_stream') }}
  GROUP BY user_id, event_time
),

user_activity AS (
  SELECT DISTINCT user_id, day FROM all_events
),

activity_lagged AS (
  SELECT
    user_id,
    day,
    LAG(day) OVER (PARTITION BY user_id ORDER BY day) AS prev_day
  FROM user_activity
),

current_period AS (
  SELECT
    user_id,
    day,
    CASE
      WHEN day = DATE_TRUNC(DATE(first_seen), DAY) THEN 'new'
      WHEN prev_day IS NOT NULL THEN 'retained'
      ELSE 'resurrected'
    END AS status
  FROM activity_lagged
  LEFT JOIN (
    SELECT user_id, MIN(event_time) AS first_seen
    FROM {{ source('Events', 'event_stream') }}
    GROUP BY user_id
  ) fs ON activity_lagged.user_id = fs.user_id
),

churned_users AS (
  SELECT
    user_id,
    prev_day AS day,
    'churned' AS status
  FROM activity_lagged
  WHERE prev_day IS NOT NULL AND user_id NOT IN (
    SELECT user_id FROM current_period
  )
)

SELECT
  day,
  status,
  COUNT(DISTINCT user_id) AS users
FROM (
  SELECT * FROM current_period
  UNION ALL
  SELECT * FROM churned_users
)
GROUP BY day, status
ORDER BY day, status;
