{{ config(materialized='view') }}

--CTE: Getting the first transaction dates for cohort
WITH user_cohorts_weekly AS (
  SELECT
    user_id,
    MIN(DATE_TRUNC(DATE(event_time), WEEK(SUNDAY))) AS cohort_week
  FROM {{ source('Events','event_stream') }}
  GROUP BY user_id
),

--CTE: setting user events  
user_events_weekly AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(event_time), WEEK(SUNDAY)) AS event_week
  FROM {{ source('Events','event_stream') }}
  GROUP BY user_id, event_week
),
cohort_activity AS (
  SELECT
    uc.cohort_week,
    ue.event_week,
    ue.user_id
  FROM user_cohorts_weekly uc
  JOIN user_events_weekly ue USING(user_id)
),
cohort_counts AS (
  SELECT
    cohort_week,
    DATE_DIFF(event_week, cohort_week, WEEK) AS period_offset,
    COUNT(DISTINCT user_id) AS users
  FROM cohort_activity
  GROUP BY cohort_week, period_offset
),
--CTE: Getting the Cohort Size
cohort_size AS (
  SELECT
    cohort_week,
    users AS size
  FROM cohort_counts
  WHERE period_offset = 0
)

--Final: Creating the cohort weekly tally
SELECT
  DATE_TRUNC(DATE(cc.cohort_week), month) cal_month,
  cc.cohort_week AS cohort_week_date,
  cc.period_offset,
  cc.users,
  cs.size AS cohort_size,
  SAFE_DIVIDE(cc.users, cs.size) AS retention_rate
FROM cohort_counts cc
JOIN cohort_size cs USING(cohort_week)
ORDER BY cohort_week, period_offset
