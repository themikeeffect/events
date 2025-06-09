{{
  config(
    materialized='table'
  )
}}

-- CTE: one day-per-user from raw events
WITH all_data AS (
  SELECT
    ad.user_id,
    DATE_TRUNC(DATE(ad.event_time), DAY) AS day
  FROM {{ source('Events','event_stream') }} ad
  GROUP BY ad.user_id, DATE_TRUNC(DATE(ad.event_time), DAY) 
),

--CTE: daily aggregates of transactions and engagements
user_activity AS (
  SELECT 
  DATE_TRUNC(DATE(ua.event_time), DAY) AS day,
  ua.user_id,
  COUNT(event_time) trns_activity,  
  COUNT(
      CASE 
        WHEN ua.event_type ='reward_search' then event_time
        WHEN ua.event_type IN ('miles_earned','miles_redeemed') Then NULL
        else event_time
      END
  )  activity,  
  COUNT(
      CASE 
        WHEN ua.event_type IN ('miles_earned','miles_redeemed') Then event_time
        else NULL
      END
  )  trns,
  sum(
    CASE 
      WHEN ua.event_type = 'miles_earned' then miles_amount
      ELSE 0
    END
  ) miles_earned,
  sum(
    CASE 
      WHEN ua.event_type = 'miles_redeemed' then miles_amount
      ELSE 0
    END
  ) miles_redeemed
  FROM {{ source('Events','event_stream') }} ua
GROUP BY
  DATE_TRUNC(DATE(ua.event_time), DAY),
  ua.user_id
),

--CTE: identifying first and last transactions
logged_data AS (
  SELECT 
    ld.user_id,
    ld.day,
    MIN(ld.day) first_seen,
    MAX(ld.day) last_trns
  FROM all_data ld
  GROUP BY
    ld.user_id,
    ld.day
),

--CTE: joining calendar days to user span and adding previous transaction days
with_calendar_days as (
  SELECT 
    c.user_id,
    c.cal_day,
    c.trns_day,
    LAG(c.trns_day) OVER (PARTITION BY c.user_id ORDER BY c.cal_day) AS prev_day,
    MIN(c.first_seen) OVER (PARTITION BY c.user_id) AS first_seen,
    MAX(c.trns_day) OVER (PARTITION BY c.user_id) AS last_trns
    FROM(
    SELECT 
      f.user_id,
      f.cal_day,
      g.day trns_day,
      g.first_seen
    FROM (
      SELECT DISTINCT
        e.user_id,
        d.day cal_day
      FROM {{ ref('calendar') }} AS d
      LEFT JOIN logged_data e
      ON d.day >= e.day
    ) f
    LEFT JOIN logged_data g
    ON f.user_id = g.user_id
    AND f.cal_day = g.day
  ) c 
), 

range_raw as (
  SELECT DISTINCT h.user_id, h.first_seen, h.last_trns
  FROM with_calendar_days h
),

-- End Table: Classificaiton of New Flag
final as (
    SELECT 
    grw.user_id,
    grw.cal_day,
    grw.trns_day,
    grw.prev_day,
    -- New User Indicator
    CASE WHEN DATE_TRUNC(DATE(dim_user.first_event_time), DAY) = grw.cal_day
        THEN TRUE
        ELSE FALSE
    END is_new_user,    
    p_ua.trns_activity,
    p_ua.activity,
    p_ua.trns,
    p_ua.miles_earned,
    p_ua.miles_redeemed
  FROM with_calendar_days grw
  INNER JOIN range_raw j
  ON grw.user_id = j.user_id
  AND grw.cal_day between j.first_seen and j.last_trns
  LEFT JOIN user_activity p_ua
  ON grw.user_id = p_ua.user_id
  AND grw.cal_day = p_ua.day
  LEFT JOIN {{ ref('dim_users') }} dim_user
  ON dim_user.user_id = grw.user_id
    
)

SELECT *
FROM final f
ORDER BY user_id, cal_day

