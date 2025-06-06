-- DAU --
WITH all_events_day AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(event_time), DAY) AS day,
    MIN(event_time) OVER (PARTITION BY user_id) AS first_seen
   FROM {{ source('Events', 'event_stream') }}
  GROUP BY user_id, event_time
),

user_activity_day AS (
  SELECT DISTINCT user_id, day FROM all_events_day
),

activity_lagged_day AS (
  SELECT
    user_id,
    day,
    LAG(day) OVER (PARTITION BY user_id ORDER BY day) AS prev_day
  FROM user_activity_day
),

current_period_day AS (
  SELECT
    a.user_id,
    a.day,
    CASE
  WHEN a.day = DATE_TRUNC(DATE(fs.first_seen), DAY) THEN 'new'
  WHEN a.prev_day = DATE_SUB(a.day, INTERVAL 1 DAY) THEN 'retained'
  ELSE 'resurrected'
    END AS status
  FROM activity_lagged_day a
  LEFT JOIN (
    SELECT user_id, MIN(event_time) AS first_seen
     FROM {{ source('Events', 'event_stream') }}
    GROUP BY user_id
  ) fs ON a.user_id = fs.user_id
),

churned_users_day AS (
  SELECT
    prev.user_id,
    prev.day,
    'churned' AS status
  FROM user_activity_day prev
  LEFT JOIN user_activity_day next
    ON prev.user_id = next.user_id
   AND next.day = DATE_ADD(prev.day, INTERVAL 1 DAY)
  WHERE next.user_id IS NULL
),
--- WAU ----
all_events_week AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(event_time), WEEK(MONDAY)) AS week,
    MIN(event_time) OVER (PARTITION BY user_id) AS first_seen
   FROM {{ source('Events', 'event_stream') }}
  GROUP BY user_id, event_time
),

user_activity_week AS (
  SELECT DISTINCT user_id, week FROM all_events_week
),

activity_lagged_week AS (
  SELECT
    user_id,
    week,
    LAG(week) OVER (PARTITION BY user_id ORDER BY week) AS prev_week
  FROM user_activity_week
),

current_period_week AS (
  SELECT
    a.user_id,
    a.week,
    CASE
      WHEN a.week = DATE_TRUNC(DATE(fs.first_seen), WEEK(MONDAY)) THEN 'new'
      WHEN a.prev_week IS NOT NULL THEN 'retained'
      ELSE 'resurrected'
    END AS status
  FROM activity_lagged_week a
  LEFT JOIN (
    SELECT user_id, MIN(event_time) AS first_seen
     FROM {{ source('Events', 'event_stream') }}
    GROUP BY user_id
  ) fs ON a.user_id = fs.user_id
),

churned_users_week AS (
  SELECT
    prev.user_id,
    prev.week,
    'churned' AS status
  FROM user_activity_week prev
  LEFT JOIN user_activity_week next
    ON prev.user_id = next.user_id
   AND next.week = DATE_ADD(prev.week, INTERVAL 1 WEEK)
  WHERE next.user_id IS NULL
),

--- DAU ----
all_events_month AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(event_time), MONTH) AS month,
    MIN(event_time) OVER (PARTITION BY user_id) AS first_seen
   FROM {{ source('Events', 'event_stream') }}
  GROUP BY user_id, event_time
),

user_activity_month AS (
  SELECT DISTINCT user_id, month FROM all_events_month
),

activity_lagged_month AS (
  SELECT
    user_id,
    month,
    LAG(month) OVER (PARTITION BY user_id ORDER BY month) AS prev_month
  FROM user_activity_month
),

current_period_month AS (
  SELECT
    a.user_id,
    a.month,
    CASE
      WHEN a.month = DATE_TRUNC(DATE(fs.first_seen), MONTH) THEN 'new'
      WHEN a.prev_month IS NOT NULL THEN 'retained'
      ELSE 'resurrected'
    END AS status
  FROM activity_lagged_month a
  LEFT JOIN (
    SELECT user_id, MIN(event_time) AS first_seen
     FROM {{ source('Events', 'event_stream') }}
    GROUP BY user_id
  ) fs ON a.user_id = fs.user_id
),

churned_users_month AS (
  SELECT
    prev.user_id,
    prev.month,
    'churned' AS status
  FROM user_activity_month prev
  LEFT JOIN user_activity_month next
    ON prev.user_id = next.user_id
   AND next.month = DATE_ADD(prev.month, INTERVAL 1 MONTH)
  WHERE next.user_id IS NULL
),
--- Main Joining Query ---
main AS (
SELECT
  'Day' as metric,
  DATE_TRUNC(DATE(day), MONTH) AS month,
  day as period,
  status,
  COUNT(DISTINCT user_id) AS users
FROM (
  SELECT * FROM current_period_day
  UNION ALL
  SELECT * FROM churned_users_day
)
GROUP BY day, status

UNION ALL

SELECT
  'Week' as metric,
  DATE_TRUNC(DATE(week), MONTH) AS month,
  week as period,
  status,
  COUNT(DISTINCT user_id) AS users
FROM (
  SELECT * FROM current_period_week
  UNION ALL
  SELECT * FROM churned_users_week
)
GROUP BY week, status

UNION ALL

SELECT
  'Month' as metric,
  month,
  month as period,
  status,
  COUNT(DISTINCT user_id) AS users
FROM (
  SELECT * FROM current_period_month
  UNION ALL
  SELECT * FROM churned_users_month
)
GROUP BY month, status
)

SELECT *
FROM main



