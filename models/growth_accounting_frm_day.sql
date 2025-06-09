{{
  config(
    materialized='table'
  )
}}

-- FINAL: getting all statuses
SELECT 
    gt.user_id,
    DATE_TRUNC(DATE(gt.cal_day), WEEK(SUNDAY)) cal_week, 
    DATE_TRUNC(DATE(gt.cal_day), MONTH) cal_month,
    gt.cal_day,
    gt.trns_day,
    gt.prev_day,
    -- User Classification
    CASE 
        WHEN gt.is_new_user is TRUE then 1
    END new_user,
    CASE 
        when gt.cal_day = trns_day then 1
    END active,
    CASE 
        WHEN gt.is_new_user is TRUE then NULL
        WHEN gt.cal_day = gt.trns_day and gt.prev_day is null then NULL
        WHEN gt.cal_day = gt.trns_day and gt.prev_day is not null then 1
        ELSE NULL
    END retained,
    CASE 
        WHEN gt.is_new_user is TRUE then NULL
        WHEN gt.cal_day = gt.trns_day and gt.prev_day is null then 1
        ELSE NULL
    END resurrected,
    CASE 
        WHEN gt.is_new_user is TRUE then NULL
        WHEN gt.trns_day is null and gt.prev_day is not null then 1
        ELSE NULL
    END churned,
    -- Transaction Type Indicator
    CASE
        WHEN gt.activity > 0 AND gt.trns = 0 THEN 'Engagement'
        WHEN gt.activity = 0 AND gt.trns > 0 THEN 'Miles'
        WHEN gt.activity > 0 AND gt.trns > 0 THEN 'Miles & Engagement'
        WHEN gt.trns_day is null and gt.prev_day is not null then 'Churned'
        ELSE 'No Activity'
    END trns_type,    
    -- Transaction Sub Type Indicator
    CASE
        WHEN gt.activity > 0 AND gt.trns = 0 THEN 'Engagement'
        WHEN gt.activity = 0 AND gt.trns > 0 AND miles_redeemed <> 0 AND miles_redeemed <> 0 THEN 'Miles Earned & Redeemed'
        WHEN gt.activity = 0 AND gt.trns > 0 AND miles_earned = 0 THEN 'Miles Redemption Only'
        WHEN gt.activity = 0 AND gt.trns > 0 AND miles_redeemed = 0 THEN 'Miles Earned Only'
        WHEN gt.activity > 0 AND gt.trns > 0 AND miles_redeemed <> 0 AND miles_redeemed <> 0 THEN 'Engagement & Earn & Redeem'
        WHEN gt.activity > 0 AND gt.trns > 0 AND miles_earned = 0 THEN 'Engagement & Miles Redemption'
        WHEN gt.activity > 0 AND gt.trns > 0 AND miles_redeemed = 0 THEN 'Engagement & Miles Earned'
        WHEN gt.trns_day is null and gt.prev_day is not null then 'Churned'
    ELSE 'No Activity'
    END trns_sub_type,    
    gt.trns_activity,
    gt.activity,
    gt.trns,
    gt.miles_earned,
    gt.miles_redeemed    
FROM {{ ref('growth_transactions') }} gt


