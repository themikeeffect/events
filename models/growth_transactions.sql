{{
  config(
    materialized='view'
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
    p_ld.user_id,
    cal.day       AS cal_day,
    ad.day        AS trns_day,
    LAG(ad.day)  OVER (PARTITION BY p_ld.user_id ORDER BY cal.day) AS prev_day,
    p_ld.first_seen
  FROM {{ ref('calendar') }} AS cal
  JOIN logged_data AS p_ld
    ON cal.day BETWEEN p_ld.first_seen AND p_ld.last_trns
  LEFT JOIN all_data AS ad
    ON ad.user_id = p_ld.user_id
   AND ad.day    = cal.day
), 

-- End Table: Classificaiton of New Flag, transaction type, and identifying all activities
final as (
    SELECT 
    grw.user_id,
    grw.cal_day,
    grw.trns_day,
    grw.prev_day,
    -- New User Indicator
    CASE 
        WHEN grw.cal_day = grw.trns_day THEN TRUE
        ELSE FALSE
    END is_new_user,
    -- Transaction Type Indicator
    CASE
      WHEN p_ua.activity > 0 AND p_ua.trns = 0 THEN 'Engagement'
      WHEN p_ua.activity = 0 AND p_ua.trns > 0 THEN 'Miles'
      WHEN p_ua.activity > 0 AND p_ua.trns > 0 THEN 'Miles/Engagement'
      ELSE 'No Activity'
    END trns_type,    
    -- Transaction Sub Type Indicator
    CASE
      WHEN p_ua.activity > 0 AND p_ua.trns = 0 THEN 'Engagement Only'
      WHEN p_ua.activity = 0 AND p_ua.trns > 0 AND miles_redeemed <> 0 AND miles_redeemed <> 0 THEN 'Miles Earned & Redeemed'
      WHEN p_ua.activity = 0 AND p_ua.trns > 0 AND miles_earned = 0 THEN 'Miles Redemption Only'
      WHEN p_ua.activity = 0 AND p_ua.trns > 0 AND miles_redeemed = 0 THEN 'Miles Earned Only'
      WHEN p_ua.activity > 0 AND p_ua.trns > 0 AND miles_redeemed <> 0 AND miles_redeemed <> 0 THEN 'Engagement & Earn & Redeem'
      WHEN p_ua.activity > 0 AND p_ua.trns > 0 AND miles_earned = 0 THEN 'Engagement & Miles Redemption'
      WHEN p_ua.activity > 0 AND p_ua.trns > 0 AND miles_redeemed = 0 THEN 'Engagement & Miles Earned'
      ELSE 'No Activity'
    END trns_sub_type,       
    p_ua.trns_activity,
    p_ua.activity,
    p_ua.trns,
    p_ua.miles_earned,
    p_ua.miles_redeemed
  FROM with_calendar_days grw
  LEFT JOIN user_activity p_ua
  ON grw.user_id = p_ua.user_id
  AND grw.cal_day = p_ua.day
    
)

SELECT *
FROM final f
ORDER BY user_id, cal_day
