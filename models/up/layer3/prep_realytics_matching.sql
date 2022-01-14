#StandardSQL
# Dataset: bi_preparation
# Table name: prep_analytics_matching
/* 
 * The purpose of the following query is to keep the unique couple (id_prospect ; uiser_id).
 * It allows to deduplicate the users.
 */

WITH union_table AS(
/* Union of the app & call events 
 * One row per user x event
 */
	SELECT   
	  timestamp
	, ry_user_id
	, id_prospect
	, event
	, current_domain
	, current_page
	, referrer
	, device
	, CAST(IFNULL(device_type, 0) AS INT64) AS device_type
	, os
	, source
	, is_mobile
	, ip_address
	, is_organic
	, channel_attribution
	, ccode
	FROM {{ref('prep_realytics_app_events')}} 
	UNION ALL
	SELECT 
	  timestamp
	, ry_user_id
	, id_prospect
	, event
	, current_domain
	, current_page
	, referrer
	, device
	, CAST(IFNULL(device_type, 0) AS INT64) AS device_type
	, os
	, source
	, is_mobile
	, ip_address
	, is_organic
	, channel_attribution
	, ccode
	FROM {{ref('prep_realytics_call_event')}}
)


, yesterday_data AS (
/* Retrieve yesterday's data and keep an unique id_prospect per ry_user_id 
 * One row per user x id_prospect
 */
  SELECT DISTINCT
    ry_user_id
  , first_timestamp
  , FIRST_VALUE(id_prospect IGNORE NULLS) OVER sorted_prospect AS id_prospect
  FROM(
    SELECT 
      ry_user_id
    , id_prospect
    , reference_date
    , MIN(timestamp) AS first_timestamp
    FROM union_table
    LEFT JOIN `souscritoo-1343.star_schema.attribution` AS attr
      USING(id_prospect)
    WHERE DATE(union_table.timestamp) = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
    GROUP BY union_table.ry_user_id, union_table.id_prospect, attr.reference_date
  ) 

  WINDOW sorted_prospect AS(
    PARTITION BY ry_user_id
    ORDER BY id_prospect LIKE '001%' DESC, id_prospect LIKE '00Q%' DESC, reference_date DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  )
  
)


, prep_final_table AS(
/* Union of the table prep_realytics_matching in BI and yesterday's data
 * This sub-query built an unique ry_user_id if an id_prospect is linked to several ry_user_id in yesterday's data
 * It updates the value of the id_prospect if it changed between yesterday and previous days (Ex: id_app_user became SF account)
 * One row per user x id_prospect
 */
  SELECT DISTINCT
    ry_user_id
  , id_prospect
  , first_timestamp 
  , FIRST_VALUE(ry_user_id IGNORE NULLS) OVER sorted_uid AS unique_uid

  FROM yesterday_data 

  WINDOW sorted_uid AS(
    PARTITION BY id_prospect
    ORDER BY DATE(first_timestamp), ry_user_id LIKE '%.%' DESC, first_timestamp 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
  )

  UNION ALL

  SELECT 
    ry_user_id 
  , MIN(COALESCE(crm_account.id_crm, crm_lead.id_crm, crm_app.id_crm, matching.id_prospect)) OVER(PARTITION BY ry_user_id, first_timestamp) AS id_prospect
  , first_timestamp
  , unique_uid

  FROM  `souscritoo-1343.bi_preparation.prep_realytics_matching` AS matching

  LEFT JOIN `souscritoo-1343.star_schema.dimension_crm` AS crm_account
    ON matching.id_prospect = crm_account.id_crm

  LEFT JOIN `souscritoo-1343.star_schema.dimension_crm` AS crm_lead
    ON matching.id_prospect = crm_lead.id_sf_lead 

  LEFT JOIN `souscritoo-1343.star_schema.dimension_crm` AS crm_app
    ON matching.id_prospect = crm_app.id_app_client
) 


, final_table AS (
/* Retrieve the min timestamp per unique_uid x id_prospect
 * One row per user x id_prospect
 */
	SELECT DISTINCT
	  ry_user_id
	, id_prospect
	, MIN(first_timestamp) OVER(PARTITION BY unique_uid, id_prospect) AS first_timestamp
	, unique_uid
	FROM prep_final_table
)


, unique_user_id AS (
/* For each couple (id_prospect, ry_user_id), the sub-query retrieves the unique_uid 
 * that may have been used in the past.
 * One row per user x id_prospect
 */
  SELECT DISTINCT 
    ry_user_id 
  , id_prospect
  , first_timestamp
  , FIRST_VALUE(unique_uid) OVER sorted_event AS unique_uid

  FROM final_table

  WHERE id_prospect IS NOT NULL

  WINDOW sorted_event AS (
  PARTITION BY id_prospect
  ORDER BY first_timestamp
  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
  )
)


------------------------------------------------------------------------------------------------------------------------------------------------------
/* Final table */
------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT 
  unique_uid AS ry_user_id 
, id_prospect
, FIRST_VALUE(first_timestamp) OVER sorted_event AS first_timestamp
, unique_uid
, FIRST_VALUE(id_prospect) OVER sorted_event AS unique_id_prospect

FROM unique_user_id

WHERE id_prospect IS NOT NULL

WINDOW sorted_event AS (
	PARTITION BY unique_uid
	ORDER BY first_timestamp
	ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
)

ORDER BY first_timestamp 