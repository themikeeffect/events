{{
  config(
    materialized='table'
  )
}}

-- CTE: getting all statuses for weekly growth accounting
    --- generating the user movement
WITH gt_raw_movement as (
SELECT 
    gt_raw.cal_month,
    gt_raw.cal_week,
    gt_raw.user_id,
    gt_raw.no_trns_wk,    
    CASE WHEN gt_raw.no_trns_wk > 0 then LAG(gt_raw.cal_week) OVER (PARTITION BY gt_raw.user_id ORDER BY cal_week) END trns_prev_week,
    CASE WHEN gt_raw.no_trns_wk > 0 then gt_raw.cal_week END trns_week,
    gt_raw.first_trns_wk,
    gt_raw.last_trns_wk,
    gt_raw.is_new_user,
    gt_raw.trns_activity,
    gt_raw.activity,
    gt_raw.trns,
    gt_raw.miles_earned,
    gt_raw.miles_redeemed
FROM(
SELECT 
    DATE_TRUNC(DATE(gt.cal_day), MONTH) cal_month,
    DATE_TRUNC(DATE(gt.cal_day), Week(SUNDAY)) cal_week, 
    gt.user_id,
    SUM(
        CASE WHEN gt.trns_day is not null THEN 1
        ELSE 0
    END) no_trns_wk,
    SUM(
        CASE WHEN gt.is_new_user is TRUE THEN 1
        ELSE 0
    END) is_new_user,    
    MIN(gt.trns_day) first_trns_wk,
    MAX(gt.trns_day) last_trns_wk,
    sum(gt.trns_activity) trns_activity,
    sum(gt.activity) activity,
    sum(gt.trns) trns,
    sum(gt.miles_earned) miles_earned,
    sum(gt.miles_redeemed) miles_redeemed
FROM {{ ref('growth_transactions') }} gt
GROUP BY
    DATE_TRUNC(DATE(gt.cal_day), Week(SUNDAY)), 
    DATE_TRUNC(DATE(gt.cal_day), MONTH),
    gt.user_id
) gt_raw
),

-- CTE: getting all statuses
growth_weekly as (
SELECT 
    gt.user_id,
    gt.cal_month,
    gt.cal_week,
    gt.trns_week,
    gt.trns_prev_week,
    gt.no_trns_wk,    
 -- User Classification
    CASE 
        WHEN gt.is_new_user = 1 then gt.user_id
    END new_user,
    CASE 
        when gt.cal_week = gt.trns_week then gt.user_id
    END active,
    CASE 
        WHEN gt.is_new_user = 1 then NULL
        WHEN gt.cal_week = gt.trns_week and gt.trns_prev_week is null then NULL
        WHEN gt.cal_week = gt.trns_week and gt.trns_prev_week is not null then gt.user_id
        ELSE NULL
    END retained,
    CASE 
        WHEN gt.is_new_user = 1 then NULL
        WHEN gt.cal_week = gt.trns_week and gt.trns_prev_week is null then user_id
        ELSE NULL
    END resurrected,
    CASE 
        WHEN gt.is_new_user = 1 then NULL
        WHEN gt.trns_week is null and gt.trns_prev_week is not null then user_id
        ELSE NULL
    END churned,
    -- Transaction Type Indicator
    CASE
        WHEN gt.activity > 0 AND gt.trns = 0 THEN 'Engagement'
        WHEN gt.activity = 0 AND gt.trns > 0 THEN 'Miles'
        WHEN gt.activity > 0 AND gt.trns > 0 THEN 'Miles & Engagement'
        WHEN gt.trns_week is null and gt.trns_prev_week is not null then 'Churned'
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
        WHEN gt.trns_week is null and gt.trns_prev_week is not null then 'Churned'
    ELSE 'No Activity'
    END trns_sub_type,
    gt.trns_activity,
    gt.activity,
    gt.trns,
    gt.miles_earned,
    gt.miles_redeemed    
FROM gt_raw_movement gt
),

final as (
SELECT 
  l.cal_week,
  l.cal_month,  
  l.trns_type,
  l.trns_sub_type,
  COUNT(DISTINCT l.active) active,
  COUNT(DISTINCT l.new_user) new_users,  
  COUNT(DISTINCT l.retained) retained,  
  COUNT(DISTINCT l.resurrected) resurrected, 
  COUNT(DISTINCT l.churned) pos_churned,
  COUNT(DISTINCT l.churned)*-1 neg_churned,
  sum(l.trns_activity) trns_activity,
  sum(l.activity) activity,
  sum(l.trns) trns,
  sum(l.miles_earned) miles_earned,
  sum(l.miles_redeemed) miles_redeemed
FROM growth_weekly l 
GROUP BY 
  l.cal_week,
  l.cal_month,    
  l.trns_type,
  l.trns_sub_type
)

SELECT *
FROM(
SELECT
  cal_month,
  cal_week,
  trns_type,
  trns_sub_type,
  CASE 
    WHEN user_type = 'tot_churned' THEN 'TOTAL'
    ELSE 'BREAKDOWN' 
  END type,
  user_type,
  user_cnt
FROM final 
UNPIVOT (
  user_cnt  FOR user_type IN (
    new_users   AS 'new_users',
    retained    AS 'retained',
    resurrected AS 'resurrected',
    pos_churned as 'tot_churned',
    neg_churned as 'churned',
    active as 'active'
  )
)
ORDER BY
  cal_month,
  cal_week,
  user_type,
  trns_type,
  trns_sub_type
) a 

WHERE 
    ( 
       (trns_type <> 'Churned' AND NOT user_type IN ('churned','tot_churned'))
    OR ( trns_type = 'Churned' AND user_type IN ('churned','tot_churned') )
    )


