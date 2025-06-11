{{ config(materialized='view') }}

--CTE: Getting the first transaction dates for cohort
WITH user_cohorts_daily AS (
  SELECT
    user_id,
    MIN(DATE(event_time)) AS cohort_date
  FROM {{ source('Events','event_stream') }}
  GROUP BY user_id
),

--CTE: setting user events  
user_events_daily AS (
  SELECT
    user_id,
    DATE(event_time) AS event_date
  FROM {{ source('Events','event_stream') }}
  GROUP BY user_id, event_date
),
cohort_activity AS (
  SELECT
    uc.cohort_date,
    ue.event_date,
    ue.user_id
  FROM user_cohorts_daily uc
  JOIN user_events_daily ue USING(user_id)
),
cohort_counts AS (
  SELECT
    cohort_date,
    event_date,
    DATE_DIFF(event_date, cohort_date, DAY) AS period_offset,
    COUNT(DISTINCT user_id) AS users
  FROM cohort_activity
  GROUP BY cohort_date, event_date, period_offset
),
--CTE: Getting the Cohort Size
cohort_size AS (
  SELECT
    cohort_date,
    users AS size
  FROM cohort_counts
  WHERE period_offset = 0
)

--Final: Creating the cohort daily tally
SELECT
  DATE_TRUNC(DATE(cc.event_date), month) cal_month,
  DATE_TRUNC(DATE(cc.cohort_date), month) cohort_ref_month,
  cc.cohort_date,
  cc.period_offset,
  cc.users,
  cs.size AS cohort_size,
  SAFE_DIVIDE(cc.users, cs.size) AS retention_rate
FROM cohort_counts cc
JOIN cohort_size cs USING(cohort_date)
ORDER BY cohort_date, period_offset
