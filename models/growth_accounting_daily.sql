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
    a.user_id,
    a.day,
    CASE
  WHEN a.day = DATE_TRUNC(DATE(fs.first_seen), DAY) THEN 'new'
  WHEN a.prev_day = DATE_SUB(a.day, INTERVAL 1 DAY) THEN 'retained'
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
    prev.user_id,
    prev.day,
    'churned' AS status
  FROM user_activity prev
  LEFT JOIN user_activity next
    ON prev.user_id = next.user_id
   AND next.day = DATE_ADD(prev.day, INTERVAL 1 DAY)
  WHERE next.user_id IS NULL
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
ORDER BY day, status