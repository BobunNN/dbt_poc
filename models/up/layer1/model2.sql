{{ config(materialized='table', target='dev2') }}

SELECT
  account_name
  , account_id
  , property_name
  , users
  , device_category
  , source_medium
  , search_destination_page
  , view_name
  , date
  , view_id
FROM
  `souscritoo-1343.souscritoo_bi.bi_analyticsreports`
WHERE
  (date >= '2021-06-07' OR view_id IS NOT NULL)
UNION ALL
SELECT
  *
FROM
  `souscritoo-1343.souscritoo_bi.prep_analyticsreports`
WHERE
  view_id IS NOT NULL