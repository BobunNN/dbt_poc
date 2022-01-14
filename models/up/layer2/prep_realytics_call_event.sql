#StandardSQL
# Dataset: bi_preparation
# Table name: prep_realytics_call_event
/* 
 * The purpose of the following query is to get the call events.
 */

WITH analytics_user_report AS (
/* Clean the analytics user report
 * One row per user x event (pageview, click)
 */
   SELECT *
    FROM(

        SELECT 
          session_id
        , device_category AS device
        , platform
        , data_source AS ry_source
        , CAST(session_date AS DATE) AS date
        , user_id AS ry_user_id
        , CAST(activity_time AS TIMESTAMP) AS timestamp
        , CASE
            WHEN source = 'google' THEN 'https://www.google.com/'
            WHEN source = 'bing' THEN 'https://www.bing.com/'
            WHEN source <> '(direct)' THEN CONCAT('https://www.', source)
            ELSE source
          END AS source
        , medium
        , channel_grouping
        , CASE WHEN campaign IN ('(not set)', '(not provided)') THEN NULL ELSE campaign END AS campaign
        , CASE WHEN keyword IN ('(not set)', '(not provided)') THEN NULL ELSE keyword END AS keyword
        , hostname AS domain_name
        , CASE 
            WHEN hostname = 'www.papernest.com' AND landing_page_path = '(not set)' AND event_category = 'Scroll Down' THEN  CONCAT('www.papernest.com', event_action)
            WHEN hostname = 'www.papernest.com' AND IFNULL(landing_page_path, '') NOT LIKE 'www.papernest.com%' THEN CONCAT(hostname, REGEXP_EXTRACT(landing_page_path, r'([^?]*)'))
            WHEN hostname = 'www.papernest.com.googleweblight.com' THEN CONCAT('www.papernest.com', REGEXP_EXTRACT(landing_page_path, r'([^?]*)'))
            WHEN landing_page_path = '(not set)' THEN NULL 
            ELSE REGEXP_EXTRACT(landing_page_path,  r'([^?]*)') 
          END AS landing_page_path
        , CASE 
            WHEN page_path IS NULL AND event_category = 'Scroll Down' THEN CONCAT('www.papernest.com', event_action)
            WHEN page_path IS NULL AND hostname = 'www.papernest.com' AND IFNULL(landing_page_path, '') NOT LIKE 'www.papernest.com%' THEN CONCAT(hostname, REGEXP_EXTRACT(landing_page_path, r'([^?]*)'))
            WHEN page_path IS NULL THEN REGEXP_EXTRACT(landing_page_path,  r'([^?]*)') 
            WHEN hostname = 'www.papernest.com' AND IFNULL(page_path, '') NOT LIKE 'www.papernest.com%' THEN CONCAT(hostname, REGEXP_EXTRACT(page_path, r'([^?]*)'))
            WHEN hostname = 'www.papernest.com.googleweblight.com' THEN CONCAT('www.papernest.com', REGEXP_EXTRACT(page_path, r'([^?]*)'))
            WHEN page_path = '(not set)' THEN NULL 
            ELSE REGEXP_EXTRACT(page_path, r'([^?]*)') 
          END AS page_path
        , LOWER(activity_type) AS event
        , CASE WHEN event_category = 'null' THEN NULL ELSE event_category END AS event_category
        , CASE 
            WHEN event_action = 'null' THEN NULL 
            WHEN event_category = 'Scroll Down' THEN CONCAT('www.papernest.com', event_action)
            ELSE event_action 
          END AS event_action
        , CASE 
            WHEN event_label = 'null' THEN NULL 
            WHEN event_label = '(not set)' THEN NULL
            ELSE event_label 
          END AS event_label
        
        FROM `souscritoo-1343.souscritoo_bi.bi_userreports`  
        
        WHERE NOT (landing_page_path = '(not set)' AND page_path IS NULL)
        AND IFNULL(hostname, '') NOT IN ('calendly.com', 'staging.papernest.com', 'www.papernest.com.googleweblight.com', 'view2.copyscape.com', 'www.copyscape.com', 'www.souscritoo.com')
        AND hostname NOT LIKE '%www-papernest-com.translate.goog%'
        AND IFNULL(Page_path, '') NOT LIKE '%.html'
        )

    WHERE date = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
    AND IFNULL(event_category, '') NOT IN ('salesforce_audience', 'Télechargement EDL', 'Scroll', 'Scroll Down', 'CTA Link Click', 'EDL Download', 'Document Download')
    AND NOT (event = 'event' AND event_action IS NULL AND event_label IS NULL)


)


, analytics_ry_events AS (
 /* Right formatting of the analytics report to feed Realytics' algorithm
  * One row per user x event (pageview, click)
  */
    SELECT  
     DATETIME(analytics.timestamp) AS timestamp
    , analytics.ry_user_id
    , CAST(NULL AS STRING) AS id_app_user
    , FIRST_VALUE(matching.id_prospect) OVER sorted_prospect AS id_prospect
    , CASE
        WHEN analytics.event = 'event' AND analytics.event_category = 'All clicks to app from papernest.com' THEN 'webapp_launch'
        WHEN analytics.event = 'event' AND analytics.event_action LIKE '%app__button%' THEN 'webapp_launch'
        WHEN analytics.event = 'event' AND analytics.event_action IS NULL AND LOWER(analytics.event_category) LIKE '%en ligne%' THEN 'webapp_launch'
        WHEN analytics.event = 'event' AND analytics.event_action LIKE '%call__button%' THEN 'call'
        WHEN analytics.event = 'event' AND (analytics.event_action LIKE '%call-back%' OR analytics.event_action LIKE '%CallBack%') THEN 'callback'
        WHEN analytics.event = 'event' AND LOWER(analytics.event_label) LIKE '%rappel%'  THEN 'callback'
        WHEN analytics.event = 'event' AND REGEXP_CONTAINS(REPLACE(analytics.event_label, ' ', ''), '[0-9]+') THEN 'call'
        WHEN analytics.event = 'pageview' THEN 'ry_page'
        ELSE analytics.event
      END AS event
    , REGEXP_EXTRACT(analytics.page_path, r'([^\/]*)') AS current_domain
    , CASE 
        WHEN analytics.page_path NOT LIKE '%http%' THEN CONCAT('https://', analytics.page_path) 
        ELSE analytics.page_path 
      END AS current_page
    , CASE 
        WHEN analytics.landing_page_path = analytics.page_path AND source = '(direct)' THEN NULL
        WHEN analytics.landing_page_path = analytics.page_path THEN source
        ELSE analytics.landing_page_path 
      END AS referrer
    , analytics.device
    , CASE
       WHEN analytics.device = 'desktop' THEN 0
       WHEN analytics.device = 'mobile' THEN 1
       WHEN analytics.device = 'tablet' THEN 2
       ELSE NULL
      END AS device_type
    , analytics.platform AS os
    , analytics.ry_source AS source
    , CASE WHEN analytics.device = 'mobile' THEN TRUE ELSE FALSE END AS is_mobile
    , CASE 
        WHEN REGEXP_CONTAINS(analytics.landing_page_path, r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)') THEN TRUE 
        WHEN REGEXP_CONTAINS(analytics.page_path , r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)') THEN TRUE 
        WHEN analytics.landing_page_path LIKE '%www.papernest.com/' THEN TRUE 
        WHEN analytics.page_path LIKE '%www.papernest.com/' THEN TRUE 
        WHEN REGEXP_CONTAINS(analytics.landing_page_path, r'my\.papernest\.com\/(?:papernest|papernest-1)\/') THEN TRUE 
        WHEN REGEXP_CONTAINS(analytics.page_path, r'my\.papernest\.com\/(?:papernest|papernest-1)\/') THEN TRUE 
        ELSE FALSE 
      END AS is_organic
    , CAST(NULL AS STRING) AS ip_address
    , CASE 
        WHEN analytics.channel_grouping = 'Direct' THEN 0
        WHEN analytics.channel_grouping = 'Organic Search' THEN 1
        WHEN analytics.channel_grouping = 'Paid Search' THEN 2
        WHEN analytics.channel_grouping = 'Social' THEN 4
        WHEN analytics.channel_grouping = 'Referral' THEN 6
        ELSE -1
      END AS channel_attribution
    
    FROM analytics_user_report AS analytics
    
    LEFT JOIN `souscritoo-1343.bi_preparation.prep_analytics_prospect_matching` AS matching
      ON analytics.ry_user_id = matching.ry_user_id
      AND DATETIME(analytics.timestamp) >= matching.start_timestamp 
      AND DATETIME(analytics.timestamp) < matching.end_timestamp

    WINDOW sorted_prospect AS(
      PARTITION BY matching.ry_user_id
      ORDER BY matching.start_timestamp DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      )
 
)

, wannaspeak_data AS (
  /* Retrieve the Wannaspeak data only for the brand phone numbers & clean it
   * One row per id_prospect x call
   */
  SELECT DISTINCT
    wsc.sct_created AS timestamp
  , REGEXP_EXTRACT(wsc.var8, r',GA1\.2\.([0-9]*\.[0-9]*)') AS ry_user_id
  , CAST(NULL AS STRING) AS id_app_user
  , FIRST_VALUE(attr.id_prospect) OVER sorted_prospect AS id_prospect #several id_prospect could match to a unique ga_client_id that's why the FIRST_VALUE function is used
  , 'call' AS event
  , REPLACE(REGEXP_EXTRACT(wsc.var2, r'https?:\/\/([^/]*)'), '%C3%A9', 'é') AS current_domain
  , REPLACE(REGEXP_EXTRACT(wsc.var2, r'([^?]*)'), '%C3%A9', 'é') AS current_page
  , REGEXP_EXTRACT(wsc.var3, r'([^?]*)') AS referrer
  , CASE
      WHEN wsc.device = 'c' THEN 'desktop'
      WHEN wsc.device = 'm' THEN 'mobile'
      WHEN wsc.device = 't' THEN 'tablet'
      WHEN LOWER(wsc.var4) NOT LIKE '%ua%' THEN wsc.var4
      WHEN LOWER(wsc.var4) LIKE '%adsbot-google%' THEN NULL
      ELSE NULL
    END AS device 
  , CASE
      WHEN wsc.device = 'c' THEN 0
      WHEN wsc.device = 'm' THEN 1
      WHEN wsc.device = 't' THEN 2
      ELSE NULL
    END AS device_type
  , CAST(NULL AS STRING) AS os
  , 'web' AS source
  , CASE WHEN wsc.device = 'm' OR LOWER(wsc.device) LIKE '%mobile%' THEN TRUE ELSE FALSE END AS is_mobile
  , CASE WHEN wsc.sct_num IN ('0033189058989', '0033184890077', '0033805900900') THEN TRUE ELSE FALSE END AS is_organic
  , CAST(NULL AS STRING) AS ip_address
  , FIRST_VALUE(CASE
      WHEN attr.business_unit = 'SEO' THEN 1
      WHEN attr.source = 'sea' THEN 2
      WHEN attr.source = 'organic_acquisition' THEN 0
      WHEN attr.source = 'display' THEN 8
      ELSE -1
    END) OVER sorted_prospect AS channel_attribution
  
  FROM `souscritoo-1343.souscritoo_bi.bi_cleaned_wannaspeakcall` AS wsc
  
  LEFT JOIN `souscritoo-1343.souscritoo_bi.bi_vectorial_attribution` AS attr
     ON attr.id_calltracking = wsc.id_wsc
     
  WHERE var2 LIKE '%papernest.com%'
  AND DATE(wsc.sct_created) = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
  AND IFNULL(wsc.sct_num, '') LIKE '0033%'
  AND wsc.var2 NOT LIKE 'https://curl.papernest.com%'
  AND IFNULL(wsc.var3, '') <> 'https://www.googleadservices.com/pagead/aclk'
  AND attr.id_prospect IS NOT NULL

  WINDOW sorted_prospect AS(
      PARTITION BY wsc.id_wsc
      ORDER BY attr.id_prospect LIKE '001%' DESC, attr.id_prospect LIKE '00Q%' DESC, attr.sct_created DESC, attr.id_prospect
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  )
)


, wannaspeak_ry_events AS (
 /* Right formatting of the Wannaspeak data to feed Realytics' algorithm : Use an unique ry_user_id
  * One row per user x event
  */
    SELECT DISTINCT
     wsc.timestamp
    , COALESCE(wsc.ry_user_id, matching.ry_user_id) AS ry_user_id
    , wsc.id_app_user
    , wsc.id_prospect
    , wsc.event
    , wsc.current_domain
    , CASE 
        WHEN wsc.current_page LIKE 'https://%' THEN wsc.current_page
        WHEN wsc.current_page LIKE 'http://%' THEN REPLACE(wsc.current_page, 'http', 'https')
        ELSE CONCAT('https://', wsc.current_page)
      END AS current_page
    , wsc.* EXCEPT(timestamp, ry_user_id, id_app_user, id_prospect, event, current_domain, current_page)
    
    FROM wannaspeak_data AS wsc
    
    LEFT JOIN `souscritoo-1343.bi_preparation.prep_analytics_prospect_matching` AS matching
      ON wsc.id_prospect = matching.id_prospect
      AND DATETIME(wsc.timestamp) >= matching.start_timestamp 
      AND DATETIME(wsc.timestamp) < matching.end_timestamp
 )


, webcallback_data AS (
  /* Retrieve the Webcallback data only for the brand phone numbers & clean it
   * One row per id_prospect x call
   */
    SELECT DISTINCT
      DATETIME(wcb.sct_created) AS timestamp
    , CAST(NULL AS STRING) AS user_id
    , CAST(NULL AS STRING) AS id_app_user
    , FIRST_VALUE(attr.id_prospect) OVER sorted_prospect AS id_prospect #several id_prospect could match to a unique ga_client_id that's why the FIRST_VALUE function is used
    , 'callback' AS event
    , REGEXP_EXTRACT(wcb.url, r'https?:\/\/([^/]*)') AS current_domain
    , REPLACE(REGEXP_EXTRACT(wcb.url, r'([^?]*)'), '%C3%A9', 'é') AS current_page
    , CAST(NULL AS STRING) AS referrer
    , CAST(NULL AS STRING) AS device
    , CAST(NULL AS INT64) AS device_type
    , CAST(NULL AS STRING) AS os
    , 'web' AS source
    , CAST(NULL AS BOOL) AS is_mobile
    , CASE 
        WHEN REGEXP_CONTAINS(wcb.url, r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)') THEN TRUE 
        WHEN wcb.url LIKE '%www.papernest.com/' THEN TRUE 
        WHEN REGEXP_CONTAINS(wcb.url, r'my\.papernest\.com\/(?:papernest|papernest-1)\/') THEN TRUE 
        ELSE FALSE 
      END AS is_organic
    , wcb.ip AS ip_address
    , FIRST_VALUE(CASE
        WHEN attr.business_unit = 'SEO' THEN 1
        WHEN attr.source = 'sea' THEN 2
        WHEN attr.source = 'organic_acquisition' THEN 0
        WHEN attr.source = 'display' THEN 8
        ELSE -1
      END) OVER sorted_prospect AS channel_attribution
    
    FROM `souscritoo-1343.souscritoo_bi.bi_cleaned_webcallback` AS wcb
    
    LEFT JOIN `souscritoo-1343.souscritoo_bi.bi_vectorial_attribution` AS attr
      ON wcb.id_wcb = attr.id_calltracking 
      
    WHERE wcb.url LIKE '%papernest.com%'
    AND wcb.country = 'France'
    AND DATE(wcb.sct_created) = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
    AND IFNULL(wcb.business_unit, '') <> 'Partnerships'
    AND attr.id_prospect NOT LIKE 'pap%'
    
    WINDOW sorted_prospect AS (
      PARTITION BY wcb.id_wcb
      ORDER BY attr.id_prospect LIKE '001%' DESC, attr.id_prospect LIKE '00Q%' DESC, wcb.sct_created DESC, attr.id_prospect
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)


, webcallback_ry_events AS (
 /* Right formatting of the Webcallback data to feed Realytics' algorithm: Use an unique ry_user_id
  * One row per user x event
  */
    SELECT DISTINCT
      wcb.timestamp
    , matching.ry_user_id
    , wcb.* EXCEPT(timestamp, user_id)
    FROM webcallback_data AS wcb

    LEFT JOIN `souscritoo-1343.bi_preparation.prep_analytics_prospect_matching` AS matching
      ON wcb.id_prospect = matching.id_prospect
      AND DATETIME(wcb.timestamp) >= matching.start_timestamp 
      AND DATETIME(wcb.timestamp) < matching.end_timestamp
  )


, gfn_ry_events AS (
  /* Retrieve the Webcallback data only for the brand phone numbers & clean it
   * One row per id_prospect x call
   */
    SELECT DISTINCT
      dim_call.call_start AS timestamp
    , MIN(ft_call.id_prospect) OVER(PARTITION BY dim_call.id_call) AS ry_user_id
    , CAST(NULL AS STRING) AS id_app_user
    , MIN(ft_call.id_prospect) OVER(PARTITION BY dim_call.id_call) AS id_prospect
    , 'call' AS event
    , 'www.google.com' AS current_domain
    , 'https://www.google.com/' AS current_page
    , CAST(NULL AS STRING) AS referrer
    , 'mobile' AS device
    , 1 AS device_type
    , CAST(NULL AS STRING) AS os
    , 'web' AS source
    , TRUE AS is_mobile
    , CASE 
        WHEN dim_call.service = 'S_Call_SEA_TV_Add' THEN TRUE 
        WHEN dim_call.sct_number IN ('0033189058989', '0033184890077', '0033805900900') THEN TRUE
        ELSE FALSE 
      END AS is_organic
    , CAST(NULL AS STRING) AS ip_address
    , 2 AS channel_attribution

    FROM `souscritoo-1343.star_schema.dimension_phone_call` AS dim_call
    JOIN `souscritoo-1343.star_schema.fact_table_call` AS ft_call
      ON dim_call.id_call = ft_call.id_dim_phone_call 

    WHERE dim_call.country = 'France'
    AND (ft_call.id_dim_call_tracking_gfn IS NOT NULL
      OR dim_call.service = 'S_Call_SEA_TV_Add'
      OR dim_call.sct_number IN ('0033189058989', '0033184890077', '0033805900900'))
    AND DATE(dim_call.call_start) = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
)


, prep_matching_analytics_wannaspeak AS (
  /* Find additionnal matches between ry_user_id in analytics report and our id_prospect in Wannaspeak tables.
   * The idea is to get the id_prospect who are not linked to an ry_user_id and make a match 
   * using the timestamp.
   *
   * The event call is retrieved in the analytics report and the associated ry_user_id is linked
   * to an id_prospect if the timestamp difference between noth events is less tha  30 seconds.
   *
   * One row per ry_user_id x id_prospect x timestamp
   */

    SELECT DISTINCT 
      analytics.ry_user_id
    , FIRST_VALUE(wannaspeak_ry_events.timestamp) OVER sorted_event AS timestamp
    , FIRST_VALUE(wannaspeak_ry_events.id_prospect) OVER sorted_event AS id_prospect 
    FROM wannaspeak_ry_events

    LEFT JOIN (
      SELECT 
        ry_user_id
      , timestamp
      , current_page
      FROM analytics_ry_events
      WHERE event = 'call'
      AND NOT EXISTS(SELECT DISTINCT 1 FROM wannaspeak_ry_events WHERE analytics_ry_events.ry_user_id = wannaspeak_ry_events.ry_user_id)
    ) AS analytics
      ON analytics.current_page = wannaspeak_ry_events.current_page
      AND wannaspeak_ry_events.timestamp BETWEEN analytics.timestamp AND DATETIME_ADD(analytics.timestamp, INTERVAL 30 SECOND)

    WHERE wannaspeak_ry_events.ry_user_id IS NULL
    AND  analytics.ry_user_id IS NOT NULL

    WINDOW sorted_event AS(
      PARTITION BY analytics.ry_user_id, DATE(wannaspeak_ry_events.timestamp)
      ORDER BY wannaspeak_ry_events.id_prospect LIKE '001%' DESC, wannaspeak_ry_events.id_prospect LIKE '00Q%' DESC, wannaspeak_ry_events.timestamp DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)

, matching_analytics_wannaspeak AS (
  /* If an id_prospect is linked to 2 ry_user_id on the same day, only the first ry_user_id is kept
   * One row per id_prospect 
   */
  SELECT DISTINCT
    id_prospect
  , FIRST_VALUE(timestamp) OVER sorted_event AS timestamp
  , FIRST_VALUE(ry_user_id) OVER sorted_event AS ry_user_id
  FROM prep_matching_analytics_wannaspeak
  WINDOW sorted_event AS (
    PARTITION BY id_prospect
    ORDER BY timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
  )
)



, prep_matching_analytics_webcallback AS (
  /* Find additionnal matches between ry_user_id in analytics report and our id_prospect in Webcallback tables.
   * The idea is to get the id_prospect who are not linked to an ry_user_id and make a match 
   * using the timestamp.
   *
   * The event "callback" is retrieved in the analytics report and the associated ry_user_id is linked
   * to an id_prospect if the timestamp difference between noth events is less tha  30 seconds.
   *
   * One row per ry_user_id x id_prospect x timestamp
   */

    SELECT DISTINCT 
      analytics.ry_user_id
    , FIRST_VALUE(webcallback_ry_events.timestamp) OVER sorted_event AS timestamp
    , webcallback_ry_events.id_prospect
    FROM webcallback_ry_events

    LEFT JOIN (
      SELECT ry_user_id
      , timestamp
      , current_page
      FROM analytics_ry_events
      WHERE event = 'callback'
      AND NOT EXISTS(SELECT DISTINCT 1 FROM webcallback_ry_events WHERE analytics_ry_events.ry_user_id = webcallback_ry_events.ry_user_id)
   ) AS analytics
      ON analytics.current_page = webcallback_ry_events.current_page
      AND webcallback_ry_events.timestamp BETWEEN DATETIME_SUB(analytics.timestamp, INTERVAL 1 SECOND) AND DATETIME_ADD(analytics.timestamp, INTERVAL 10 SECOND)

    WHERE webcallback_ry_events.ry_user_id IS NULL
    AND  analytics.ry_user_id IS NOT NULL

    WINDOW sorted_event AS(
      PARTITION BY analytics.ry_user_id, id_prospect, DATE(webcallback_ry_events.timestamp)
      ORDER BY webcallback_ry_events.id_prospect LIKE '001%' DESC, webcallback_ry_events.id_prospect LIKE '00Q%' DESC, webcallback_ry_events.timestamp DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)


, matching_analytics_webcallback AS (
  /* If an id_prospect is linked to 2 ry_user_id on the same day, only the first ry_user_id is kept
   * One row per id_prospect 
   */
  SELECT DISTINCT
    id_prospect
  , FIRST_VALUE(timestamp) OVER sorted_event AS timestamp
  , FIRST_VALUE(ry_user_id) OVER sorted_event AS ry_user_id
  FROM prep_matching_analytics_webcallback
  WINDOW sorted_event AS (
    PARTITION BY id_prospect
    ORDER BY timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
  )
)


, call_events AS (
  /* Wannaspeak events & add the ry_user_id if it's possible when it's missing
   * One row per event x timestamp x uid
   */
    SELECT 
      wannaspeak.timestamp
    , wannaspeak.ry_user_id
    , wannaspeak.id_app_user
    , wannaspeak.id_prospect
    , wannaspeak.event
    , COALESCE(wannaspeak.current_domain, analytics.current_domain) AS current_domain
    , COALESCE(wannaspeak.current_page, analytics.current_page) AS current_page
    , COALESCE(wannaspeak.referrer, analytics.referrer) AS referrer
    , IFNULL(COALESCE(wannaspeak.device, analytics.device), 'mobile') AS device
    , IFNULL(COALESCE(wannaspeak.device_type, analytics.device_type), 1) AS device_type
    , COALESCE(wannaspeak.os, analytics.os) AS os
    , COALESCE(wannaspeak.source, analytics.source) AS source
    , IFNULL(COALESCE(wannaspeak.is_mobile, analytics.is_mobile), TRUE) AS is_mobile
    , COALESCE(wannaspeak.is_organic, analytics.is_organic) AS is_organic
    , COALESCE(wannaspeak.ip_address, analytics.ip_address) AS ip_address
    , COALESCE(analytics.channel_attribution, wannaspeak.channel_attribution) AS channel_attribution

  FROM(
    SELECT 
      wannaspeak_ry_events.timestamp
    , COALESCE(wannaspeak_ry_events.ry_user_id, matching.ry_user_id) AS ry_user_id
    , wannaspeak_ry_events.* EXCEPT(timestamp, ry_user_id)
    FROM wannaspeak_ry_events
    LEFT JOIN matching_analytics_wannaspeak AS matching
      ON matching.id_prospect = wannaspeak_ry_events.id_prospect
      AND wannaspeak_ry_events.timestamp >= matching.timestamp
  ) AS wannaspeak

  LEFT JOIN analytics_ry_events AS analytics
    ON analytics.ry_user_id = wannaspeak.ry_user_id
    AND analytics.timestamp BETWEEN wannaspeak.timestamp AND DATETIME_ADD(wannaspeak.timestamp, INTERVAL 30 SECOND)
)


, callback_events AS (
  /* Webcallback events & add the ry_user_id if it's possible when it's missing
   * One row per event x timestamp x uid
   */
    SELECT DISTINCT
      webcallback.timestamp
    , webcallback.ry_user_id
    , webcallback.id_app_user
    , webcallback.id_prospect
    , webcallback.event
    , COALESCE(webcallback.current_domain, analytics.current_domain) AS current_domain
    , COALESCE(webcallback.current_page, analytics.current_page) AS current_page
    , COALESCE(webcallback.referrer, analytics.referrer) AS referrer
    , COALESCE(webcallback.device, analytics.device) AS device
    , COALESCE(webcallback.device_type, analytics.device_type) AS device_type
    , COALESCE(webcallback.os, analytics.os) AS os
    , COALESCE(webcallback.source, analytics.source) AS source
    , COALESCE(webcallback.is_mobile, analytics.is_mobile) AS is_mobile
    , COALESCE(webcallback.is_organic, analytics.is_organic) AS is_organic
    , COALESCE(webcallback.ip_address, analytics.ip_address) AS ip_address
    , COALESCE(analytics.channel_attribution, webcallback.channel_attribution) AS channel_attribution

    FROM(
      SELECT 
        webcallback_ry_events.timestamp
      , COALESCE(webcallback_ry_events.ry_user_id, matching.ry_user_id) AS ry_user_id
      , webcallback_ry_events.* EXCEPT(timestamp, ry_user_id)
      FROM webcallback_ry_events
      LEFT JOIN matching_analytics_webcallback AS matching
        ON matching.id_prospect = webcallback_ry_events.id_prospect
        AND webcallback_ry_events.timestamp >= matching.timestamp
    ) AS webcallback

    LEFT JOIN analytics_ry_events AS analytics
      ON analytics.ry_user_id = webcallback.ry_user_id
      AND analytics.timestamp BETWEEN webcallback.timestamp AND DATETIME_ADD(webcallback.timestamp, INTERVAL 10 SECOND)
)


, union_table AS (
  /* Union of all call tables + the pageview of the analytics report
   * One row per event x timestamp x uid
   */
    SELECT DISTINCT *
    FROM(
      SELECT *
      FROM analytics_ry_events
      WHERE event = 'ry_page'
      UNION ALL
      SELECT *
      FROM call_events
      UNION ALL
      SELECT *
      FROM callback_events
      UNION ALL
      SELECT *
      FROM gfn_ry_events
    )
)


, opportunity_creation AS (
  /* Create the event "started_flow" when the prospect called and an opportunity was created
   * One row per user
   */
    SELECT DISTINCT
      DATETIME(opport.createddate, opport.created_time) AS timestamp
    , FIRST_VALUE(call.ry_user_id) OVER sorted_event AS ry_user_id
    , call.id_app_user
    , call.id_prospect
    , 'started_flow' AS event
    , FIRST_VALUE(call.current_domain) OVER sorted_event AS current_domain
    , FIRST_VALUE(call.current_page) OVER sorted_event AS current_page
    , FIRST_VALUE(call.referrer) OVER sorted_event AS referrer
    , FIRST_VALUE(call.device) OVER sorted_event AS device
    , FIRST_VALUE(call.device_type) OVER sorted_event AS device_type
    , FIRST_VALUE(call.os) OVER sorted_event AS os
    , FIRST_VALUE(call.source) OVER sorted_event AS source
    , FIRST_VALUE(call.is_mobile) OVER sorted_event AS is_mobile
    , FIRST_VALUE(call.is_organic) OVER sorted_event AS is_organic
    , FIRST_VALUE(call.ip_address) OVER sorted_event AS ip_address
    , FIRST_VALUE(call.channel_attribution) OVER sorted_event AS channel_attribution
    , opport.id_sf
    FROM `souscritoo-1343.star_schema.dimension_salesforce_opportunity` AS opport
    JOIN union_table AS call
      ON opport.accountid = call.id_prospect
      AND DATETIME(opport.createddate, opport.created_time) BETWEEN call.timestamp AND DATETIME_ADD(call.timestamp, INTERVAL 30 MINUTE)
    WHERE call.event = 'call'

    WINDOW sorted_event AS(
      PARTITION BY opport.accountid, opport.createddate
      ORDER BY call.timestamp DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)


, validation_opportunity AS(
  /* Create the event "validated_contract" when the prospect called and an opportunity was validated
   * One row per user
   */

    SELECT DISTINCT
      MIN(DATETIME(history.CreatedDate)) OVER(PARTITION BY opport.id_sf) AS timestamp
    , opport.ry_user_id
    , opport.id_app_user
    , opport.id_prospect
    , 'validated_contract' AS event
    , opport.* EXCEPT(timestamp, ry_user_id, id_app_user, id_prospect, event, id_sf)
    FROM opportunity_creation AS opport 
    JOIN `souscritoo-1343.raw_airflow_tables.salesforce_opportunityfieldhistory` AS history
      ON opport.id_sf = history.OpportunityId 
    WHERE Field IN ('StageName') 
    AND (NewValue IN ('Contrat validÃƒÂ© par le client','Contrat validÃƒÂ© par le fournisseur','Contrat validé par le client','Contrat validé par le fournisseur')
      OR OldValue IN ('Contrat validÃƒÂ© par le client','Contrat validÃƒÂ© par le fournisseur','Contrat validé par le client', 'Contrat validé par le fournisseur')) 
    OR Field IN ('ContractValidation__c','TECH_Validation__c') AND (NewValue IN ('NET','BRUT') OR OldValue IN ('NET','BRUT'))
)


------------------------------------------------------------------------------------------------------------------------------------------------------
/* Final table */
------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT *
FROM(

  SELECT DISTINCT
    union_table.timestamp
  , COALESCE(union_table.ry_user_id, union_table.id_prospect) AS ry_user_id
  , union_table.* EXCEPT(timestamp, ry_user_id, id_app_user, channel_attribution, ip_address, is_organic)
  , CASE
      WHEN union_table.is_organic IS NOT NULL THEN is_organic 
      WHEN REGEXP_CONTAINS(union_table.current_page, r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)') THEN TRUE 
      WHEN REGEXP_CONTAINS(union_table.referrer , r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)') THEN TRUE 
      WHEN union_table.current_page LIKE '%www.papernest.com/' THEN TRUE 
      WHEN union_table.referrer LIKE '%www.papernest.com/' THEN TRUE 
      WHEN REGEXP_CONTAINS(union_table.current_page, r'my\.papernest\.com\/(?:papernest|papernest-1)\/') THEN TRUE 
      WHEN REGEXP_CONTAINS(union_table.referrer, r'my\.papernest\.com\/(?:papernest|papernest-1)\/') THEN TRUE 
      ELSE FALSE 
    END AS is_organic
  , ip_address
  , 'FR' AS ccode
  , IFNULL(union_table.channel_attribution, -1) AS channel_attribution

  FROM (
      SELECT *
      FROM union_table
      UNION ALL 
      SELECT * EXCEPT(id_sf)
      FROM opportunity_creation
      UNION ALL 
      SELECT *
      FROM validation_opportunity
  ) AS union_table

  LEFT JOIN `souscritoo-1343.star_schema.attribution` AS attr
    USING(id_prospect)
)