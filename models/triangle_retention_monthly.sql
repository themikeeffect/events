{{ config(materialized='view') }}

--CTE: Getting the first transaction dates for cohort
WITH user_cohorts_monthly AS (
  SELECT
    user_id,
    MIN(DATE_TRUNC(DATE(event_time), MONTH)) AS cohort_month
  FROM {{ source('Events','event_stream') }}
  GROUP BY user_id
),

--CTE: setting user events  
user_events_monthly AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(event_time), MONTH) AS event_month
  FROM {{ source('Events','event_stream') }}
  GROUP BY user_id, event_month
),
cohort_activity AS (
  SELECT
    uc.cohort_month,
    ue.event_month,
    ue.user_id
  FROM user_cohorts_monthly uc
  JOIN user_events_monthly ue USING(user_id)
),
cohort_counts AS (
  SELECT
    cohort_month,
    event_month,
    DATE_DIFF(event_month, cohort_month, MONTH) AS period_offset,
    COUNT(DISTINCT user_id) AS users
  FROM cohort_activity
  GROUP BY cohort_month, event_month, period_offset
),
--CTE: Getting the Cohort Size
cohort_size AS (
  SELECT
    cohort_month,
    users AS size
  FROM cohort_counts
  WHERE period_offset = 0
)

--Final: Creating the cohort monthly tally
SELECT
  DATE_TRUNC(DATE(cc.event_month), month) cal_month,
  DATE_TRUNC(DATE(cc.cohort_month), month) cohort_ref_month,  
  cc.cohort_month AS cohort_month_date,
  cc.period_offset,
  cc.users,
  cs.size AS cohort_size,
  SAFE_DIVIDE(cc.users, cs.size) AS retention_rate
FROM cohort_counts cc
JOIN cohort_size cs USING(cohort_month)
ORDER BY cohort_month, period_offset
