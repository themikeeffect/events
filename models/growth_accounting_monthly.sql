WITH all_events AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(event_time), MONTH) AS month,
    MIN(event_time) OVER (PARTITION BY user_id) AS first_seen
  FROM {{ source('Events', 'event_stream') }}
  GROUP BY user_id, event_time
),

user_activity AS (
  SELECT DISTINCT user_id, month FROM all_events
),

activity_lagged AS (
  SELECT
    user_id,
    month,
    LAG(month) OVER (PARTITION BY user_id ORDER BY month) AS prev_month
  FROM user_activity
),

current_period AS (
  SELECT
    a.user_id,
    a.month,
    CASE
      WHEN a.month = DATE_TRUNC(DATE(fs.first_seen), MONTH) THEN 'new'
      WHEN a.prev_month IS NOT NULL THEN 'retained'
      ELSE 'resurrected'
    END AS status
  FROM activity_lagged a
  LEFT JOIN (
    SELECT user_id, MIN(event_time) AS first_seen
    FROM {{ source('Events', 'event_stream') }}
    GROUP BY user_id
  ) fs ON a.user_id = fs.user_id
),

churned_users AS (
  SELECT
    user_id,
    prev_month AS month,
    'churned' AS status
  FROM activity_lagged
  WHERE prev_month IS NOT NULL AND user_id NOT IN (
    SELECT user_id FROM current_period
  )
)

SELECT
  month,
  status,
  COUNT(DISTINCT user_id) AS users
FROM (
  SELECT * FROM current_period
  UNION ALL
  SELECT * FROM churned_users
)
GROUP BY month, status
ORDER BY month, status