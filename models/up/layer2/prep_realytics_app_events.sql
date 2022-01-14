#StandardSQL
# Dataset: bi_preparation
# Table name: prep_analytics_app_event
/* 
 * The purpose of the following query is to get the app events.
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
        WHEN analytics.event = 'event' AND LOWER(analytics.event_category) LIKE '%en ligne%' THEN 'webapp_launch'
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


, app_pages AS (
  /* Clean the App pageviews
   * One row per user x pageview
   */
    SELECT DISTINCT
      MIN(COALESCE(crm.id_crm, attr.id_prospect, app.user_id)) OVER(PARTITION BY app.user_id) AS id_prospect
    , app.user_id AS id_app_user
    , app.timestamp
    , 'ry_page' AS event
    , app.context_ip AS ip_address
    , CASE 
        WHEN app.context_page_referrer IS NULL THEN REGEXP_EXTRACT(app.referrer, r'([^?]*)') 
        ELSE REGEXP_EXTRACT(app.context_page_referrer, r'([^?]*)') 
      END AS context_page_referrer
    , REGEXP_EXTRACT(app.context_page_url, r'([^?]*)') AS context_page_url
    , app.context_user_agent
    , app.page_flow
    , app.page_name
    , app.is_conversion
    , app.title
    , app.source
    , app.pole
    , app.segment
    , app.country
    , MIN(app.context_google_analytics_client_id) OVER(PARTITION BY app.user_id, DATE(app.timestamp)) AS ry_user_id

     FROM `souscritoo-1343.prod_app_sct.pages` AS app
     
     LEFT JOIN `souscritoo-1343.star_schema.dimension_crm` AS crm
      ON crm.id_app_client = app.user_id
      
     LEFT JOIN `souscritoo-1343.star_schema.attribution` AS attr 
      ON attr.id_app_user = app.user_id

     WHERE DATE(app.timestamp) = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
     AND app.user_id IS NOT NULL
     AND app.name <> 'init_segment'
     AND IFNULL(LOWER(app.source), '') NOT IN ('immo_canal', 'web_partnerships')
     AND app.is_conversion IS FALSE
     AND app.context_page_url NOT LIKE '%app.papernest.com/pro/%'
     AND app.context_page_url NOT LIKE '%ops.app.papernest.com%'
     AND app.context_page_url NOT LIKE '%papernest.es%'
)


, webapp_launch_event AS(
  /* Create the event "webapp_launch" when the user arrived in the App.
   * The context_page_referrer is the url of this event
   * One row per user x event
   */
    SELECT 
      id_prospect
    , id_app_user
    , DATETIME_SUB(timestamp, INTERVAL 1 MILLISECOND) AS timestamp
    , 'webapp_launch' AS event
    , ip_address
    , CAST(NULL AS STRING) AS context_page_referrer
    , IFNULL(context_page_referrer, context_page_url) AS context_page_url
    , * EXCEPT(id_prospect, id_app_user, timestamp, event, ip_address, context_page_referrer, context_page_url, rank)
    FROM (
      SELECT *
      , ROW_NUMBER() OVER(PARTITION BY id_app_user, DATE(timestamp) ORDER BY timestamp) AS rank
      FROM app_pages
    ) 
    WHERE rank = 1 
)


, started_flow_event AS(
  /* Create the event "started_flow" when the user started the flow
   * One row per user x event
   */
    SELECT 
      id_prospect
    , id_app_user
    , timestamp
    , 'started_flow' AS event
    , * EXCEPT(id_prospect, id_app_user, timestamp, event, rank)
    FROM (
      SELECT *
    , ROW_NUMBER() OVER(PARTITION BY id_app_user, DATE(timestamp), page_flow ORDER BY timestamp) AS rank
      FROM app_pages
      WHERE page_flow LIKE '%subscription'
    ) 
    WHERE rank = 1 
) 


, contract_validation_event AS(
  /* Create the event "contract_validation" when the user ended the flow
   * One row per user x event
   */
    SELECT 
      id_prospect
    , id_app_user
    , timestamp
    , 'validated_contract' AS event
    , * EXCEPT(id_prospect, id_app_user, timestamp, event, rank)
    FROM (
      SELECT *
    , ROW_NUMBER() OVER(PARTITION BY id_app_user, DATE(timestamp) ORDER BY timestamp) AS rank
      FROM app_pages
      WHERE (page_flow = 'energy-subscription' AND page_name = 'payment')
      OR (page_flow = 'box-subscription' AND page_name = 'summary')
      OR (page_flow = 'insurance-subscription' AND page_name ='payment_details')
      OR (page_flow = 'cellular-subscription' AND page_name ='payment_sim')
    ) 
    WHERE rank = 1 
) 


, app_ry_events AS (
  /* Union of the previous app events and cleaning with right Realytics' format
   * One row per user x event
   */
    SELECT  
      DATETIME(app_pages.timestamp) AS timestamp
    , COALESCE(app_pages.ry_user_id, matching.ry_user_id) AS ry_user_id
    , app_pages.id_app_user
    , app_pages.id_prospect
    , app_pages.event
    , REGEXP_EXTRACT(app_pages.context_page_url, r'https?:\/\/([^/]*)') AS current_domain
    , app_pages.context_page_url AS current_page
    , app_pages.context_page_referrer AS referrer
    , app_pages.context_user_agent AS device
    , CASE 
        WHEN LOWER(app_pages.context_user_agent) LIKE '%tablet%' THEN 2
        WHEN LOWER(app_pages.context_user_agent) LIKE '%mobile%' THEN 1 
        ELSE 0
      END AS device_type
    , CASE
        WHEN app_pages.context_user_agent LIKE '%Macintosh%' THEN 'Macintosh'
        WHEN app_pages.context_user_agent LIKE '%Linux%' THEN 'Linux'
        WHEN app_pages.context_user_agent LIKE '%iPhone%' OR app_pages.context_user_agent LIKE '%iPad%' THEN 'iOS'
        WHEN app_pages.context_user_agent LIKE '%Windows%' THEN 'Windows'
        ELSE NULL
      END AS os
    , 'web' AS source
    , CASE WHEN LOWER(app_pages.context_user_agent) LIKE '%mobile%' THEN TRUE ELSE FALSE END AS is_mobile
    , CASE 
        WHEN REGEXP_CONTAINS(app_pages.context_page_url, r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)') THEN TRUE 
        WHEN REGEXP_CONTAINS(app_pages.context_page_referrer , r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)') THEN TRUE 
        WHEN app_pages.context_page_url LIKE '%www.papernest.com/' THEN TRUE 
        WHEN app_pages.context_page_referrer LIKE '%www.papernest.com/' THEN TRUE 
        WHEN REGEXP_CONTAINS(app_pages.context_page_url, r'my\.papernest\.com\/(?:papernest|papernest-1)\/') THEN TRUE 
        WHEN REGEXP_CONTAINS(app_pages.context_page_referrer, r'my\.papernest\.com\/(?:papernest|papernest-1)\/') THEN TRUE 
        ELSE FALSE 
      END AS is_organic
    , app_pages.ip_address 
    , CASE
        WHEN app_pages.source = 'organic_acquisition' THEN 0
        WHEN app_pages.pole = 'SEO' or app_pages.source = 'SEO' THEN 1
        WHEN app_pages.pole = 'PPC' OR app_pages.source = 'google' OR app_pages.source LIKE '%paid_acquisition%' THEN 2
        WHEN app_pages.source = 'display' THEN 8
        WHEN app_pages.source = 'affiliation' THEN 6
        ELSE -1 
      END AS channel_attribution

    FROM (
      SELECT *
      FROM app_pages
      UNION ALL
      SELECT *
      FROM webapp_launch_event
      UNION ALL
      SELECT *
      FROM started_flow_event
      UNION ALL
      SELECT *
      FROM contract_validation_event 
    ) AS app_pages

    LEFT JOIN `souscritoo-1343.bi_preparation.prep_analytics_prospect_matching` AS matching
      ON app_pages.id_prospect = matching.id_prospect
      AND DATETIME(app_pages.timestamp) >= matching.start_timestamp 
      AND DATETIME(app_pages.timestamp) < matching.end_timestamp
)


, prep_matching_analytics_app AS (
  /* Find additionnal matches between ry_user_id in analytics report and our id_prospect in BI
   * The idea is to get the id_prospect who are not linked to an ry_user_id and make a match 
   * using the timestamp.
   *
   * The event webapp_launch is retrieved in the analytics report and the associated ry_user_id is linked
   * to an id_app_user if the timestamp difference between noth events is less tha  4 minutes.
   *
   * One row per user x event
   */

    SELECT DISTINCT
      analytics.ry_user_id
    , FIRST_VALUE(app_ry_events.timestamp) OVER sorted_event AS timestamp
    , FIRST_VALUE(app_ry_events.id_prospect) OVER sorted_event AS id_prospect
    FROM app_ry_events

    LEFT JOIN (
      SELECT 
        ry_user_id
      , timestamp
      , current_page
      FROM analytics_ry_events
      WHERE event = 'webapp_launch'
      AND NOT EXISTS(SELECT DISTINCT 1 FROM app_ry_events WHERE analytics_ry_events.ry_user_id = app_ry_events.ry_user_id)
    ) AS analytics
      ON analytics.current_page = app_ry_events.referrer
      AND app_ry_events.timestamp BETWEEN analytics.timestamp AND DATETIME_ADD(analytics.timestamp, INTERVAL 4 MINUTE)

    WHERE app_ry_events.ry_user_id IS NULL
    AND  analytics.ry_user_id IS NOT NULL

    WINDOW sorted_event AS(
      PARTITION BY analytics.ry_user_id, DATE(app_ry_events.timestamp)
      ORDER BY app_ry_events.id_prospect LIKE '001%' DESC, app_ry_events.id_prospect LIKE '00Q%' DESC, app_ry_events.timestamp DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)

, matching_analytics_app AS (
  /* If an id_prospect is linked to 2 ry_user_id on the same day, only the first ry_user_id is kept
   * One row per id_prospect 
   */
  SELECT DISTINCT
    id_prospect
  , FIRST_VALUE(timestamp) OVER sorted_event AS timestamp
  , FIRST_VALUE(ry_user_id) OVER sorted_event AS ry_user_id
  FROM prep_matching_analytics_app
  WINDOW sorted_event AS (
    PARTITION BY id_prospect
    ORDER BY timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
  )
)


, app_events AS (
  /* Aggregation of the webapp_launch event & other App events (pageview, started_flow, validated_contract)
   * Add the ry_user_id if it's possible when it's missing
   * One row per event x timestamp x uid
   */
    SELECT *
    FROM analytics_ry_events
    WHERE event = 'webapp_launch'

    UNION ALL

    SELECT 
      app_ry_events.timestamp
    , COALESCE(app_ry_events.ry_user_id, matching.ry_user_id)
    , app_ry_events.* EXCEPT(timestamp, ry_user_id)
    FROM app_ry_events
    LEFT JOIN matching_analytics_app AS matching
      ON matching.id_prospect = app_ry_events.id_prospect
      AND app_ry_events.timestamp >= matching.timestamp
    WHERE NOT (event = 'webapp_launch' AND current_domain = 'app.papernest.com') # the App should be launched from another site than app.papernest.com
 )
 

 , final_app_events AS (
  /* Flag the events to be removed (Ex: 2 consecutives "webapp_launch") & delete them 
   * One row per event x timestamp x uid
   */
   SELECT * EXCEPT(to_be_removed)
   FROM (
      SELECT *
      , IF(LAG(event) OVER sorted_event = 'webapp_launch' AND event = 'webapp_launch' AND DATETIME_DIFF(timestamp, LAG(timestamp) OVER sorted_event, MINUTE) < 10, TRUE, FALSE) AS to_be_removed
      FROM app_events

      WINDOW sorted_event AS (
        PARTITION BY COALESCE(ry_user_id, id_app_user, id_prospect), DATE(timestamp)
        ORDER BY timestamp ASC
      )
    ) 
    WHERE to_be_removed IS FALSE
)


------------------------------------------------------------------------------------------------------------------------------------------------------
/* Final table */
------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT *
FROM(

  SELECT DISTINCT
    app.timestamp
  , COALESCE(app.ry_user_id, app.id_prospect) AS ry_user_id
  , app.* EXCEPT(timestamp, ry_user_id, id_app_user, channel_attribution, ip_address, is_organic)
  , CASE
      WHEN app.is_organic IS NOT NULL THEN is_organic 
      WHEN REGEXP_CONTAINS(app.current_page, r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)') THEN TRUE 
      WHEN REGEXP_CONTAINS(app.referrer , r'www\.papernest\.com\/(?:papernest-move\/|papernest-switch\/|papernest-monitor\/|equipe\/|fournisseurs\/|app\/)') THEN TRUE 
      WHEN app.current_page LIKE '%www.papernest.com/' THEN TRUE 
      WHEN app.referrer LIKE '%www.papernest.com/' THEN TRUE 
      WHEN REGEXP_CONTAINS(app.current_page, r'my\.papernest\.com\/(?:papernest|papernest-1)\/') THEN TRUE 
      WHEN REGEXP_CONTAINS(app.referrer, r'my\.papernest\.com\/(?:papernest|papernest-1)\/') THEN TRUE 
      ELSE FALSE 
    END AS is_organic
  , ip_address
  , 'FR' AS ccode
  , IFNULL(app.channel_attribution, -1) AS channel_attribution

  FROM final_app_events AS app

  LEFT JOIN `souscritoo-1343.star_schema.attribution` AS attr
    USING(id_prospect)

  WHERE IFNULL(current_domain, '') NOT IN ('app.papernest.es', 'staging.app.papernest.com', 'pro.papernest.com')
)