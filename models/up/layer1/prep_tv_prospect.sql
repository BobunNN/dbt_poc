#StandardSQL
# Dataset : bi_preparation
# Table name: prep_tv_prospect
/* 
 * The purpose of the following query is to identify tv prospects
 * and qualify those coming directly after seeing a tv spot
 */


WITH prospects_tv AS (
  /* Filtering prospects and actions from tv
   * One row per prospect x actions
   */


  SELECT 
    id_prospect
  , sct_created AS brand_joined_at
  , CASE
      WHEN service IN ('S_Call_Brand_AccueilFooter', 'S_Call_SEA_TV_Add') THEN 'call'
      ELSE 'app'
      END AS app_or_call
  , CASE
      WHEN REGEXP_CONTAINS(url, r'www\.papernest\.com\/(?:edf-souscription-contract\/|etat-des-lieux\/|edf-ouverture-compteur\/|edf-changement-locataire\/)') THEN FALSE
      WHEN REGEXP_CONTAINS(referrer, r'www\.papernest\.com\/(?:edf-souscription-contract\/|etat-des-lieux\/|edf-ouverture-compteur\/|edf-changement-locataire\/)') THEN FALSE
      WHEN url LIKE '%app.papernest.com/pro/%' THEN FALSE
      ELSE TRUE
    END AS is_organic
  , device
  FROM `souscritoo-1343.souscritoo_bi.bi_vectorial_attribution` AS vect_attrib
  WHERE ((vect_attrib.source = 'brand' 
            AND vect_attrib.source2 = 'tv')
          OR (vect_attrib.source = 'organic_acquisition' 
            AND vect_attrib.source2 = 'papernest.com'
            AND ((vect_attrib.referrer LIKE '%papernest.com%'
              AND REGEXP_CONTAINS(IFNULL(vect_attrib.url, ''), r'https://www.papernest.com\/(?:etat-des-lieux)\/.*') IS FALSE )
            OR (IFNULL(vect_attrib.referrer, '') IN ('https://www.google.com/', 'https://www.google.fr/', 'https://www.google.fr', 'https://www.bing.com/', 'https://www.ecosia.org/')
              AND vect_attrib.url LIKE 'https://www.papernest.com%'
              AND REGEXP_CONTAINS(vect_attrib.url, r'https://www.papernest.com\/(?:etat-des-lieux)\/.*') IS FALSE )
            OR app_account_creation
            OR crm_file_creation
            OR service IN ('S_Call_Brand_AccueilFooter', 'S_Call_SEA_TV_Add') )))
  AND vect_attrib.id_prospect IN (
        SELECT id_prospect 
        FROM `souscritoo-1343.star_schema.attribution` AS attrib
        WHERE attrib.joined_at < '2021-01-01'
          OR (attrib.joined_at > '2021-07-11' AND IFNULL(attrib.ProductType__C, '') NOT IN ('AppPro', 'ApiPro'))
        )
  AND NOT EXISTS(
        SELECT * FROM (
          SELECT id_prospect 
          FROM 
            (SELECT
              DISTINCT id_prospect
            , MAX(actual_created_date) OVER (PARTITION BY id_prospect) AS most_recent
            FROM `souscritoo-1343.star_schema.fact_table_contract`)  
          WHERE most_recent > '2021-03-01' AND most_recent < '2021-07-11') AS excluded_prospect 
        WHERE excluded_prospect.id_prospect = vect_attrib.id_prospect
        )
  )
  


, prospect_with_brand_event AS (
  /* Union with attribution and keep first action
   * One row per prospect 
   */

    SELECT DISTINCT 
      id_prospect
    , MIN(brand_joined_at) OVER (PARTITION BY id_prospect) AS brand_joined_at
    , FIRST_VALUE(prospects_tv.app_or_call) OVER prospect_first_event AS app_or_call
    , FIRST_VALUE(prospects_tv.device) OVER prospect_first_event AS device
    , attrib.joined_at
    , attrib.business_unit 
    , attrib.source 
    , attrib.source2
    FROM prospects_tv
    LEFT JOIN `souscritoo-1343.star_schema.attribution` AS attrib
      USING(id_prospect)

    WHERE brand_joined_at >= '2021-07-11'
    AND is_organic IS TRUE

    WINDOW prospect_first_event AS (
      PARTITION BY id_prospect ORDER BY brand_joined_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      )
)


, prospect_to_be_removed AS (
  /* Selection of the prospects to remove because 
   * they don't really come from the TV (opportunity in the last 3 months
   * or creation of the account in 2021 before the tv campaign)
   * One row per prospect 
   */

    SELECT DISTINCT 
      id_prospect
    FROM(
    
      SELECT DISTINCT 
        id_prospect
      , attr.sct_created
      , attr.source
      , brand_joined_at

      FROM `souscritoo-1343.souscritoo_bi.bi_vectorial_attribution` AS attr

      JOIN prospect_with_brand_event
        USING(id_prospect)

      WHERE attr.sct_created >= '2021-03-01'
      AND attr.source NOT IN ('brand', 'organic_acquisition')
    ) 
    WHERE sct_created < brand_joined_at
)


, prospect_first_contact_date AS(
  /* removing the prospects defined before
   * One row per prospect 
   */

    SELECT *
    FROM prospect_with_brand_event
    WHERE NOT EXISTS (
      SELECT 1 
      FROM prospect_to_be_removed 
      WHERE prospect_to_be_removed.id_prospect = prospect_with_brand_event.id_prospect
    )
)

 
, spots_tv_horaires AS (
  /* selection of the tv spots and their date/time 
   * One row per spot 
   */

    SELECT 
      id AS id_spot
    , DATETIME(TIMESTAMP(CONCAT(date, ' ', timestamp_spot))) AS timestamp_spot
    , DATE(TIMESTAMP(CONCAT(date, ' ', timestamp_spot))) AS jour_spot
    FROM `souscritoo-1343.gsheet_tables.gsheet_gs_tv_spots`

)


, prep_time_to_spot AS (
  /* Union of the prospects with the tv spots
   * One row per prospect x spot 
   */

    SELECT
      id_prospect
    , DATETIME(TIMESTAMP(prospect_first_contact_date.brand_joined_at), "Europe/Paris") AS brand_joined_at
    , app_or_call
    , id_spot
    , spots_tv_horaires.timestamp_spot
    , spots_tv_horaires.jour_spot
    FROM prospect_first_contact_date, spots_tv_horaires
)


, time_to_spot AS (
  /* calculation of the time difference between the brand_joined date and the tv spots
   * One row per prospect x spot 
   */

    SELECT
      id_prospect
    , brand_joined_at
    , app_or_call
    , id_spot
    , timestamp_spot
    , DATETIME_DIFF(brand_joined_at, timestamp_spot, MINUTE) AS time_diff
    , jour_spot
    FROM prep_time_to_spot
)


, closest_spot AS (
  /* Keeping only the closest spot to the brand_joined date
   * One row per prospect  
   */

    SELECT
      DISTINCT id_prospect, brand_joined_at, app_or_call
    , FIRST_VALUE(id_spot) OVER close_spot AS id_spot
    , FIRST_VALUE(timestamp_spot) OVER close_spot AS timestamp_spot
    , FIRST_VALUE(jour_spot) OVER close_spot AS jour_spot
    , FIRST_VALUE(time_diff) OVER close_spot AS time_diff
    FROM time_to_spot
    WHERE time_diff >= 0

    WINDOW close_spot AS (
      PARTITION BY id_prospect, brand_joined_at ORDER BY time_diff ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      )
)


, prospect_post_pub AS (
  /* selecting only direct prospects who come just after a spot
   * One row per prospect  
   */

    SELECT 
      COALESCE(dim_crm.id_crm,  closest_spot.id_prospect) AS id_prospect
    , brand_joined_at
    , app_or_call
    , id_spot
    , timestamp_spot
    , jour_spot
    , time_diff
    FROM closest_spot
    LEFT JOIN `souscritoo-1343.star_schema.dimension_crm` AS dim_crm
      ON closest_spot.id_prospect = dim_crm.id_app_client
    WHERE time_diff <= 13
)


, prospect_post_pub_completed AS (
  /* adding caracterisation for direct prospects and the spot they are coming from
   * One row per prospect  
   */

    SELECT 
      prospect_first_contact_date.* 
    , CASE 
        WHEN id_prospect IN (SELECT id_prospect FROM prospect_post_pub) THEN TRUE
        ELSE FALSE
        END AS is_direct_prospect
    , CASE 
        WHEN id_prospect IN (SELECT id_prospect FROM prospect_post_pub) THEN prospect_post_pub.id_spot
        ELSE NULL
        END AS id_spot
    , CASE 
        WHEN id_prospect IN (SELECT id_prospect FROM prospect_post_pub) THEN prospect_post_pub.timestamp_spot
        ELSE NULL
        END AS timestamp_spot
    ,  CASE 
        WHEN id_prospect IN (SELECT id_prospect FROM prospect_post_pub) THEN prospect_post_pub.jour_spot
        ELSE NULL
        END AS jour_spot
    , CASE 
        WHEN id_prospect IN (SELECT id_prospect FROM prospect_post_pub) THEN prospect_post_pub.time_diff
        ELSE NULL
        END AS time_diff
    FROM prospect_first_contact_date
    LEFT JOIN prospect_post_pub 
      USING (id_prospect)
)


SELECT *
FROM prospect_post_pub_completed