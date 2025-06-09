{{
  config(
    materialized='view'
  )
}}

-- CTE: getting all statuses
SELECT 
    tbl_daily.cal_day,
    tbl_daily.trns_type,
    tbl_daily.trns_sub_type,
    COUNT(
        CASE 
            WHEN tbl_daily.cal_day = tbl_daily.trns_day then tbl_daily.user_id
        END 
    ) active,
    COUNT(
        CASE 
            WHEN tbl_daily.is_new_user = TRUE THEN tbl_daily.user_id
        END
    ) new_user,
    COUNT(
        CASE 
            WHEN tbl_daily.is_new_user = TRUE then NULL
            WHEN tbl_daily.cal_day = trns_day and tbl_daily.prev_day is null then NULL
            WHEN tbl_daily.cal_day = trns_day and tbl_daily.prev_day is not null then tbl_daily.user_id
            ELSE NULL
        END 
    ) retained,    
    sum(tbl_daily.miles_earned) total_miles_earned,
    sum(tbl_daily.miles_redeemed) total_miles_redeemed
FROM {{ ref('growth_transactions') }} AS tbl_daily
GROUP BY
    tbl_daily.cal_day,
    tbl_daily.trns_type,
    tbl_daily.trns_sub_type
ORDER BY 1