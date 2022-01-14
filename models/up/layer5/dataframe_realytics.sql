#StandardSQL
# Dataset : bi_preparation
# Table name: dataframe_realytics
/* 
 * The purpose of the following query is to gather all app & call events
 * and build the dataframe that feeds the Realytics' algorithm.
 */


WITH union_table AS(
/* Union of app & call events
 * One row per timestamp x event
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
	, CASE 
			WHEN channel_attribution = 6 THEN FALSE 
			ELSE is_organic 
	  END AS is_organic
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
	, CASE 
	  	WHEN channel_attribution = 6 THEN FALSE 
			WHEN current_domain NOT LIKE '%papernest.com%' THEN FALSE
			ELSE is_organic 
	  END AS is_organic
	, channel_attribution
	, ccode
	FROM {{ref('prep_realytics_call_event')}} 
)


, papernest_data AS (
/* Creation of the "ry_profile" event that matches each unique uid with an id_prospect
 * One row per uid x id_prospect x day
 */
  SELECT DISTINCT 
    FIRST_VALUE(timestamp IGNORE NULLS) OVER sorted_event AS timestamp
  , ry_user_id AS ry_user_id
  , 'ry_profile'AS event
  , FIRST_VALUE(current_domain IGNORE NULLS) OVER sorted_event AS current_domain
  , FIRST_VALUE(current_page IGNORE NULLS) OVER sorted_event AS current_page
  , FIRST_VALUE(referrer IGNORE NULLS) OVER sorted_event AS referrer
  , FIRST_VALUE(channel_attribution IGNORE NULLS) OVER sorted_event AS channel_attribution
  , FIRST_VALUE(device IGNORE NULLS) OVER sorted_event AS device
  , FIRST_VALUE(os IGNORE NULLS) OVER sorted_event AS os
  , FIRST_VALUE(is_mobile IGNORE NULLS) OVER sorted_event AS is_mobile
  , FIRST_VALUE(is_organic IGNORE NULLS) OVER sorted_event AS is_organic
  , FIRST_VALUE(source IGNORE NULLS) OVER sorted_event AS source
  , FIRST_VALUE(device_type IGNORE NULLS) OVER sorted_event AS device_type
  , FIRST_VALUE(ip_address IGNORE NULLS) OVER sorted_event AS ip_address
  , FIRST_VALUE(ccode IGNORE NULLS) OVER sorted_event AS ccode
  , FIRST_VALUE(id_prospect IGNORE NULLS) OVER sorted_event AS id_prospect
  FROM union_table
  
  WINDOW sorted_event AS (
    PARTITION BY ry_user_id, DATE(timestamp)
    ORDER BY timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  )
)

, raw_opportunity AS(
  /* Find the conversion date from NULL to BRUT for all opportunities
   * One row per id_sf
   */

    SELECT DISTINCT
      MIN(DATETIME(history.CreatedDate)) OVER(PARTITION BY opport.id_sf) AS timestamp
    , union_table.ry_user_id
    , union_table.id_prospect

    FROM union_table 

    JOIN `souscritoo-1343.star_schema.dimension_salesforce_opportunity`  AS opport
        ON union_table.id_prospect = opport.accountid 

    JOIN `souscritoo-1343.raw_airflow_tables.salesforce_opportunityfieldhistory` AS history
      ON opport.id_sf = history.OpportunityId 

    WHERE Field IN ('StageName') 
    AND (NewValue IN ('Contrat validÃƒÂ© par le client','Contrat validÃƒÂ© par le fournisseur','Contrat validé par le client','Contrat validé par le fournisseur')
      OR OldValue IN ('Contrat validÃƒÂ© par le client','Contrat validÃƒÂ© par le fournisseur','Contrat validé par le client', 'Contrat validé par le fournisseur')) 
    OR Field IN ('ContractValidation__c','TECH_Validation__c') AND (NewValue IN ('NET','BRUT') OR OldValue IN ('NET','BRUT'))
)


, raw_client_event AS(
	/* Find the conversion date from prospect to raw client 
   * One row per id_prospect
   */
	SELECT DISTINCT
    MIN(raw_opportunity.timestamp) OVER(PARTITION BY id_prospect, ry_user_id) AS timestamp
	, FIRST_VALUE(raw_opportunity.ry_user_id) OVER sorted_event AS ry_user_id
	, raw_opportunity.id_prospect
	, 'raw_client' AS event
	, FIRST_VALUE(union_table.current_domain) OVER sorted_event AS current_domain
	, FIRST_VALUE(union_table.current_page) OVER sorted_event AS current_page
	, FIRST_VALUE(union_table.referrer) OVER sorted_event AS referrer
	, FIRST_VALUE(union_table.device) OVER sorted_event AS device
	, FIRST_VALUE(union_table.device_type) OVER sorted_event AS device_type
	, FIRST_VALUE(union_table.os) OVER sorted_event AS os
	, FIRST_VALUE(union_table.source) OVER sorted_event AS source
	, FIRST_VALUE(union_table.is_mobile) OVER sorted_event AS is_mobile
	, FIRST_VALUE(union_table.ip_address) OVER sorted_event AS ip_address
	, FIRST_VALUE(union_table.is_organic) OVER sorted_event AS is_organic
	, FIRST_VALUE(union_table.channel_attribution) OVER sorted_event AS channel_attribution
	, FIRST_VALUE(union_table.ccode) OVER sorted_event AS ccode
	  
	FROM raw_opportunity

	LEFT JOIN union_table
	  USING(ry_user_id, id_prospect)

	WHERE union_table.timestamp <= raw_opportunity.timestamp

	WINDOW sorted_event AS(
	  PARTITION BY raw_opportunity.id_prospect
	  ORDER BY union_table.timestamp DESC
	  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
	)
)


, net_opportunity AS(
  /* Find the conversion date from BRUT to NET for all opportunities
   * One row per id_sf
   */

    SELECT DISTINCT
      MIN(DATETIME(history.CreatedDate)) OVER(PARTITION BY opport.id_sf) AS timestamp
    , union_table.ry_user_id
    , union_table.id_prospect
    , opport.max_possible_revenue AS revenue
    , opport.id_sf AS id_sf_opportunity
    , 'EUR' AS currency
    , DATETIME(opport.createddate, opport.created_time) AS opportunity_creation_datetime
    
    FROM union_table 

    JOIN `souscritoo-1343.star_schema.dimension_salesforce_opportunity`  AS opport
      ON union_table.id_prospect = opport.accountid 

    JOIN `souscritoo-1343.raw_airflow_tables.salesforce_opportunityfieldhistory` AS history
      ON opport.id_sf = history.OpportunityId 

    LEFT JOIN {{ref('prep_tv_prospect')}} AS tv_attribution	
			USING(id_prospect)

  
    WHERE tv_attribution.id_prospect IS NOT NULL
    AND ((Field IN ('StageName') 
      AND NewValue IN ('Contrat validÃƒÂ© par le fournisseur','Contrat validé par le fournisseur'))
    OR (Field IN ('ContractValidation__c','TECH_Validation__c') 
      AND NewValue ='NET'))
    AND opport.validation__c = 'NET'
)


, net_client_event AS(
	/* Find the conversion date from raw_client to net client 
   * One row per id_prospect
   */
	SELECT DISTINCT
    net_opportunity.timestamp
	, FIRST_VALUE(net_opportunity.ry_user_id) OVER sorted_event AS ry_user_id
	, net_opportunity.id_prospect
	, 'net_client' AS event
	, FIRST_VALUE(union_table.current_domain) OVER sorted_event AS current_domain
	, FIRST_VALUE(union_table.current_page) OVER sorted_event AS current_page
	, FIRST_VALUE(union_table.referrer) OVER sorted_event AS referrer
	, FIRST_VALUE(union_table.device) OVER sorted_event AS device
	, FIRST_VALUE(union_table.device_type) OVER sorted_event AS device_type
	, FIRST_VALUE(union_table.os) OVER sorted_event AS os
	, FIRST_VALUE(union_table.source) OVER sorted_event AS source
	, FIRST_VALUE(union_table.is_mobile) OVER sorted_event AS is_mobile
	, FIRST_VALUE(union_table.ip_address) OVER sorted_event AS ip_address
	, FIRST_VALUE(union_table.is_organic) OVER sorted_event AS is_organic
	, FIRST_VALUE(union_table.channel_attribution) OVER sorted_event AS channel_attribution
	, FIRST_VALUE(union_table.ccode) OVER sorted_event AS ccode
  , net_opportunity.revenue
  , net_opportunity.id_sf_opportunity
  , net_opportunity.currency
	  
	FROM net_opportunity

	LEFT JOIN union_table
	  USING(ry_user_id, id_prospect)

	WHERE union_table.timestamp <= net_opportunity.opportunity_creation_datetime

	WINDOW sorted_event AS(
	  PARTITION BY net_opportunity.id_prospect, net_opportunity.id_sf_opportunity
	  ORDER BY union_table.timestamp DESC
	  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
	)
)


, prep_final_table AS (
/* Add the additional events (ry_profile, raw_client, net_client) to the other events
 * One row per timestamp x event
 */
	SELECT 
	  'ry-p4p3rne' AS apiKey
	, timestamp 
	, ry_user_id
	, event
	, current_domain AS url_domain
	, current_page AS url
	, referrer
	, channel_attribution
	, device
	, os
	, is_mobile
	, is_organic
	, source
	, source AS am
	, device_type 
	, ip_address
	, ccode
	, id_prospect
	, CAST(NULL AS FLOAT64) AS revenue
	, CAST(NULL AS STRING) AS id_sf_opportunity
	, CAST(NULL AS STRING) AS currency
	FROM union_table

	UNION ALL 

	SELECT   
	 'ry-p4p3rne' AS apiKey
	, timestamp
	, ry_user_id 
	, event 
	, current_domain AS url_domain
	, current_page AS url
	, referrer 
	, channel_attribution 
	, device 
	, os
	, is_mobile 
	, is_organic 
	, source 
	, source AS am
	, device_type 
	, ip_address 
	, ccode
	, id_prospect
	, CAST(NULL AS FLOAT64) AS revenue
	, CAST(NULL AS STRING) AS id_sf_opportunity
	, CAST(NULL AS STRING) AS currency
	FROM papernest_data
	WHERE id_prospect IS NOT NULL
  
  UNION ALL

	SELECT 
	  'ry-p4p3rne' AS apiKey
	, timestamp 
	, ry_user_id 
	, event 
	, current_domain AS url_domain
	, current_page AS url
	, referrer
	, channel_attribution
	, device
	, os
	, is_mobile
	, is_organic
	, source
	, source AS am
	, device_type
	, ip_address
	, ccode
	, id_prospect
	, CAST(NULL AS FLOAT64) AS revenue
	, CAST(NULL AS STRING) AS id_sf_opportunity
	, CAST(NULL AS STRING) AS currency
	FROM raw_client_event

	UNION ALL

	SELECT 
   'ry-p4p3rne' AS apiKey
	, timestamp 
	, ry_user_id 
	, event 
	, current_domain AS url_domain
	, current_page AS url
	, referrer 
	, channel_attribution 
	, device 
	, os
	, is_mobile 
	, is_organic 
	, source 
	, source AS am
	, device_type 
	, ip_address 
	, ccode
	, id_prospect
	, revenue
	, id_sf_opportunity
	, currency
	FROM net_client_event

)


, change_organic_value AS (
/* Change the value of the field is_organic, which is TRUE if the prospect
 * lands on the page www.papernest.com/ or the new website or the LPs
 * my.papernest.com/papernest/ & my.papernest.com/papernest-1/
 * One row per timestamp x event
 */
	SELECT DISTINCT
	  id_prospect
	, ry_user_id
	, DATE(timestamp) AS date
	, MIN(timestamp) AS first_timestamp
	, TRUE AS is_organic_value
	FROM prep_final_table
	WHERE REGEXP_CONTAINS(url, r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)')
	OR REGEXP_CONTAINS(referrer, r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)')
	OR REGEXP_CONTAINS(url, r'my\.papernest\.com\/(?:papernest|papernest-1)\/')
	OR REGEXP_CONTAINS(referrer, r'my\.papernest\.com\/(?:papernest|papernest-1)\/')
	OR url LIKE '%www.papernest.com/' 
	OR referrer LIKE '%www.papernest.com/'
	GROUP BY id_prospect, ry_user_id, date
)


------------------------------------------------------------------------------------------------------------------------------------------------------
/* Final table */
------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT *
FROM(

	SELECT DISTINCT
	  prep_final_table.apiKey
	, prep_final_table.timestamp
	, prep_final_table.ry_user_id
	, prep_final_table.event
	, prep_final_table.url_domain
	, CASE 
		WHEN prep_final_table.url LIKE 'www.papernest.com%' THEN CONCAT('https://', prep_final_table.url) 
		ELSE prep_final_table.url
	  END AS url
	, CASE 
		WHEN prep_final_table.referrer LIKE 'www.papernest.com%' THEN CONCAT('https://', prep_final_table.referrer) 
		ELSE prep_final_table.referrer
	  END AS referrer
	, prep_final_table.channel_attribution
	, CASE WHEN prep_final_table.device = 'UA' THEN NULL ELSE prep_final_table.device END AS device
	, CASE WHEN prep_final_table.os = '(not set)' THEN NULL ELSE prep_final_table.os END AS os
	, prep_final_table.is_mobile
	, CASE
		WHEN IFNULL(org.is_organic_value, FALSE) THEN TRUE 
		WHEN prep_final_table.url = 'https://www.papernest.com/' THEN TRUE 
		ELSE prep_final_table.is_organic 
	  END AS is_organic
	, prep_final_table.source
	, prep_final_table.am
	, prep_final_table.device_type
	, prep_final_table.ip_address
	, prep_final_table.ccode
	, COALESCE(ry_prospect.unique_id_prospect, prep_final_table.id_prospect) AS id_prospect
	, prep_final_table.revenue
	, prep_final_table.id_sf_opportunity
	, prep_final_table.currency

	FROM(

		SELECT DISTINCT prep_final_table.* EXCEPT(ry_user_id)
		, COALESCE(ry_uid.unique_uid, prep_final_table.ry_user_id) AS ry_user_id
		FROM prep_final_table
		LEFT JOIN {{ref('prep_realytics_matching')}} AS ry_uid
			USING(id_prospect)

	) AS prep_final_table

	LEFT JOIN {{ref('prep_realytics_matching')}} AS ry_prospect
		ON prep_final_table.ry_user_id = ry_prospect.ry_user_id

	LEFT JOIN change_organic_value AS org
		ON org.id_prospect = prep_final_table.id_prospect
		AND org.ry_user_id = prep_final_table.ry_user_id
		AND org.date = DATE(prep_final_table.timestamp)
		AND org.first_timestamp <= prep_final_table.timestamp

	WHERE IFNULL(prep_final_table.url_domain, '') NOT IN ('app.papernest.es' , 'staging.app.papernest.com', 'pro.papernest.com')
	AND DATE(prep_final_table.timestamp) = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
)
WHERE ry_user_id IS NOT NULL
AND url_domain IS NOT NULL
AND url_domain <> ''