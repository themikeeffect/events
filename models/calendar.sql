SELECT day
FROM UNNEST(
  GENERATE_DATE_ARRAY(
    (SELECT MIN(DATE(event_time)) FROM {{ source('Events', 'event_stream') }}),
    (SELECT MAX(DATE(event_time)) FROM {{ source('Events', 'event_stream') }})
  )
) AS day
