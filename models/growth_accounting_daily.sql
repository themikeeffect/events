{{
  config(
    materialized='table'
  )
}}

-- CTE: getting all statuses
WITH growth_daily as (
SELECT 
    gt.user_id,
    gt.cal_day,
    DATE_TRUNC(DATE(gt.cal_day), MONTH) cal_month, 
    gt.trns_day,
    gt.prev_day,
    -- User Classification
    CASE 
        WHEN gt.is_new_user is TRUE then gt.user_id
    END new_user,
    CASE 
        when gt.cal_day = trns_day then gt.user_id
    END active,
    CASE 
        WHEN gt.is_new_user is TRUE then NULL
        WHEN gt.cal_day = gt.trns_day and gt.prev_day is null then NULL
        WHEN gt.cal_day = gt.trns_day and gt.prev_day is not null then gt.user_id
        ELSE NULL
    END retained,
    CASE 
        WHEN gt.is_new_user is TRUE then NULL
        WHEN gt.cal_day = gt.trns_day and gt.prev_day is null then gt.user_id
        ELSE NULL
    END resurrected,
    CASE 
        WHEN gt.is_new_user is TRUE then NULL
        WHEN gt.trns_day is null and gt.prev_day is not null then gt.user_id
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
),

-- CTE: Consolidation of Tables 
consolidate_table as (
SELECT 
  l.cal_day,
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
FROM growth_daily l 
GROUP BY 
  l.cal_month,
  l.cal_day,
  l.trns_type,
  l.trns_sub_type
),

breakdown_user_cnt as (
SELECT *
FROM (
SELECT
  cal_day,
  cal_month,
  trns_type,
  trns_sub_type,
  CASE 
    WHEN user_type = 'tot_churned' THEN 'TOTAL'
    ELSE 'BREAKDOWN' 
  END type,
  user_type,
  user_cnt
FROM consolidate_table 
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
  cal_day,
  cal_month,
  user_type,
  trns_type,
  trns_sub_type
) a 

WHERE 
    ( 
       (trns_type <> 'Churned' AND NOT user_type IN ('churned','tot_churned'))
    OR ( trns_type = 'Churned' AND user_type IN ('churned','tot_churned') )
    )
),

breakdown_user_miles as (
SELECT *
FROM (
SELECT
  cal_day,
  cal_month,
  trns_type,
  trns_sub_type,
  'TOTAL' type,
  miles_trns_type,
  miles_pct
FROM consolidate_table 
UNPIVOT (
  miles_pct  FOR miles_trns_type IN (
    miles_earned   AS 'miles_earned',
    miles_redeemed    AS 'miles_redeemed'
  )
)
ORDER BY
  cal_day,
  cal_month,
  miles_trns_type,
  trns_type,
  trns_sub_type
) a 

WHERE 
   NOT (trns_type In ('Churned','No Activity')
)
)

-- final: Main table
SELECT 
    cal_day,
    cal_month,
    trns_type,
    trns_sub_type,
    type,
    user_type,
    sum(user_cnt) user_cnt,
    sum(miles_pct) miles_pct
FROM (
SELECT 
    cal_day,
    cal_month,
    trns_type,
    trns_sub_type,
    type,
    user_type,
    user_cnt,
    0 miles_pct
FROM breakdown_user_cnt

UNION ALL 
SELECT 
    cal_day,
    cal_month,
    trns_type,
    trns_sub_type,
    type,
    miles_trns_type,
    0 user_cnt,
    miles_pct
FROM breakdown_user_miles
) 
GROUP BY    cal_day,
    cal_month,
    trns_type,
    trns_sub_type,
    type,
    user_type