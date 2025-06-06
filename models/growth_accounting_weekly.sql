WITH all_events AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(event_time), WEEK(MONDAY)) AS week,
    MIN(event_time) OVER (PARTITION BY user_id) AS first_seen
  FROM {{ source('Events', 'event_stream') }}
  GROUP BY user_id, event_time
),

user_activity AS (
  SELECT DISTINCT user_id, week FROM all_events
),

activity_lagged AS (
  SELECT
    user_id,
    week,
    LAG(week) OVER (PARTITION BY user_id ORDER BY week) AS prev_week
  FROM user_activity
),

current_period AS (
  SELECT
    a.user_id,
    a.week,
    CASE
      WHEN a.week = DATE_TRUNC(DATE(fs.first_seen), WEEK(MONDAY)) THEN 'new'
      WHEN a.prev_week IS NOT NULL THEN 'retained'
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
    prev_week AS week,
    'churned' AS status
  FROM activity_lagged
  WHERE prev_week IS NOT NULL AND user_id NOT IN (
    SELECT user_id FROM current_period
  )
)

SELECT
  week,
  status,
  COUNT(DISTINCT user_id) AS users
FROM (
  SELECT * FROM current_period
  UNION ALL
  SELECT * FROM churned_users
)
GROUP BY week, status
ORDER BY week, status