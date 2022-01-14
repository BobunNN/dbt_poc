 -- this table is an aggregation of phone data on a prospect basis
-- it gathers all processing information per prospect (including processing cost)


WITH 


crm AS (
SELECT DISTINCT
    id_crm AS id_prospect
    ,sct_creation_date
FROM `souscritoo-1343.star_schema.dimension_crm`
WHERE id_crm IS NOT NULL
)

,clean_phone_call AS (
SELECT 
    id_call
    , duree_svi
    , duree_call
    , duree_wait
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id_call) AS rank
    FROM `souscritoo-1343.star_schema.dimension_phone_call`
    )
WHERE rank = 1 
    AND duree_comm > 0 AND sct_call_status = 'handled'
    AND (eloquant_campaign NOT IN ('C_Partenaires_Sales', 'C_SEO_Linking') OR eloquant_campaign IS NULL)
)

,calls AS (
SELECT
    dmt.id_call
    ,dmt.id_prospect
    ,dmt.call_datetime AS call_start
    ,CAST(dmt.call_datetime AS date) AS call_start_date
    ,dmt.direction
    ,dmt.call_status
    ,dmt.market_country
    ,IFNULL(dmt.comm_duration, 0) AS duree_comm
    ,IFNULL(dim.duree_svi, 0) AS duree_svi
    ,IFNULL(dim.duree_call, IFNULL(dmt.comm_duration, 0)) AS duree_call
    ,IFNULL(dim.duree_wait, 0) AS duree_wait
    ,IFNULL(dmt.processing_cost_v1, 0) AS processing_cost_v1
    ,IFNULL(dmt.processing_cost_v2, 0) AS processing_cost_v2
    ,IFNULL(dmt.processing_cost, 0) AS processing_cost
    ,real_or_estimated_v1 AS pc_v1_status
    ,real_or_estimated_v2 AS pc_v2_status
    ,real_or_estimated AS pc_status

FROM `souscritoo-1343.bi_datamart.datamart_daf_pc_call` AS dmt
LEFT JOIN clean_phone_call AS dim
    USING (id_call)
)


,clean_calls_with_no_prospect AS (
SELECT 
    calls.* EXCEPT (processing_cost_v1, processing_cost_v2, processing_cost)
    ,calls.processing_cost_v1 + IFNULL(calls.processing_cost_v1 * SAFE_DIVIDE(lost_costs.lost_processing_cost_v1, saved_costs.saved_processing_cost_v1), 0) AS processing_cost_v1
    ,calls.processing_cost_v2 + IFNULL(calls.processing_cost_v2 * SAFE_DIVIDE(lost_costs.lost_processing_cost_v2, saved_costs.saved_processing_cost_v2), 0) AS processing_cost_v2
    ,calls.processing_cost + IFNULL(calls.processing_cost * SAFE_DIVIDE(lost_costs.lost_processing_cost, saved_costs.saved_processing_cost), 0) AS processing_cost


FROM calls
LEFT JOIN (SELECT 
                call_start_date
                ,SUM(processing_cost_v1) AS lost_processing_cost_v1
                ,SUM(processing_cost_v2) AS lost_processing_cost_v2
                ,SUM(processing_cost) AS lost_processing_cost
            FROM calls
            WHERE id_prospect IS NULL OR NOT EXISTS (SELECT 1 FROM crm WHERE crm.id_prospect = calls.id_prospect)
            GROUP BY 1) AS lost_costs
    USING (call_start_date)
LEFT JOIN (SELECT 
                call_start_date
                ,SUM(processing_cost_v1) AS saved_processing_cost_v1
                ,SUM(processing_cost_v2) AS saved_processing_cost_v2
                ,SUM(processing_cost) AS saved_processing_cost
            FROM calls
            WHERE id_prospect IS NOT NULL AND EXISTS (SELECT 1 FROM crm WHERE crm.id_prospect = calls.id_prospect)
            GROUP BY 1) AS saved_costs
    USING (call_start_date)
WHERE NOT(id_prospect IS NULL OR id_prospect NOT IN (SELECT id_prospect FROM crm))
)


,clean_calls_miss_attribution AS (
SELECT 
    calls.* EXCEPT (processing_cost_v1, processing_cost_v2, processing_cost)
    ,calls.processing_cost_v1 + IFNULL(calls.processing_cost_v1 * SAFE_DIVIDE(wrong_costs.wrong_processing_cost_v1, wrong_costs.processing_cost_v1), 0) AS processing_cost_v1
    ,calls.processing_cost_v2 + IFNULL(calls.processing_cost_v2 * SAFE_DIVIDE(wrong_costs.wrong_processing_cost_v2, wrong_costs.processing_cost_v2), 0) AS processing_cost_v2
    ,calls.processing_cost + IFNULL(calls.processing_cost * SAFE_DIVIDE(wrong_costs.wrong_processing_cost, wrong_costs.processing_cost), 0) AS processing_cost
    ,crm.sct_creation_date

FROM crm
LEFT JOIN clean_calls_with_no_prospect AS calls
    USING (id_prospect) -- the costs due to calls with no id_prospect (so lost here) has been solve in the table clean_calls_with_no_prospect
LEFT JOIN (SELECT -- wrong_ = lost_ même logique, on considère qu'il y a un pb (wrong_) si le souscritoo id est créé plus de 3 jours après le call
                calls.call_start_date
                ,SUM(CASE WHEN date_diff(calls.call_start_date, crm.sct_creation_date, day) < -3 THEN processing_cost_v1 ELSE 0 END) AS wrong_processing_cost_v1
                ,SUM(CASE WHEN date_diff(calls.call_start_date, crm.sct_creation_date, day) >= -3 THEN processing_cost_v1 ELSE 0 END) AS processing_cost_v1
                ,SUM(CASE WHEN date_diff(calls.call_start_date, crm.sct_creation_date, day) < -3 THEN processing_cost_v2 ELSE 0 END) AS wrong_processing_cost_v2
                ,SUM(CASE WHEN date_diff(calls.call_start_date, crm.sct_creation_date, day) >= -3 THEN processing_cost_v2 ELSE 0 END) AS processing_cost_v2
                ,SUM(CASE WHEN date_diff(calls.call_start_date, crm.sct_creation_date, day) < -3 THEN processing_cost ELSE 0 END) AS wrong_processing_cost
                ,SUM(CASE WHEN date_diff(calls.call_start_date, crm.sct_creation_date, day) >= -3 THEN processing_cost ELSE 0 END) AS processing_cost
            FROM clean_calls_with_no_prospect AS calls
            LEFT JOIN crm 
            USING (id_prospect) -- the costs due to calls with no id_prospect (so lost here) has been solve in the table clean_calls_with_no_prospect
            GROUP BY 1) AS wrong_costs
    USING (call_start_date)
WHERE date_diff(calls.call_start_date, crm.sct_creation_date, day) >= -3
)


,phone_data AS (
SELECT
    id_prospect
    ,sct_creation_date
    ,count(DISTINCT id_call)                                                                            AS nb_calls
    ,SUM(IF(direction = 'in', 1 , 0))                                                                   AS nb_inbound_call
    ,SUM(IF(direction = 'out', 1 , 0))                                                                  AS nb_outbound_call
    ,SUM(IF(direction = 'in' AND call_status = 'handled', 1 , 0))                                       AS nb_handled_inbound_call
    ,SUM(IF(direction = 'out' AND call_status = 'handled', 1 , 0))                                      AS nb_handled_outbound_call
    ,SUM(CASE WHEN call_status = 'handled' THEN 1 ELSE 0 END)                                           AS nb_handled_call
    ,MIN(call_start)                                                                                    AS first_call
    ,MAX(call_start)                                                                                    AS last_call
    ,MIN(call_start_date)                                                                               AS first_call_date
    ,MAX(call_start_date)                                                                               AS last_call_date

    ,NULLIF(MIN(IF(call_status = 'handled', call_start_date , '2100-01-01')),'2100-01-01')                                                AS first_handled_call
    ,NULLIF(MIN(IF(call_status = 'handled' AND duree_comm > 60 ,call_start_date , '2100-01-01')),'2100-01-01')                             AS first_answered_date

    ,NULLIF(MIN(IF(direction='in', call_start_date ,'2100-01-01')),'2100-01-01')                                                          AS first_inbound_call_date
    ,NULLIF(MIN(IF(direction='in' AND call_status = 'handled', call_start_date,'2100-01-01')),'2100-01-01')                               AS first_handled_inbound_call_date
    ,NULLIF(MIN(IF(direction='in' AND call_status = 'handled' AND duree_comm >= 60, call_start_date,'2100-01-01')),'2100-01-01')          AS first_answered_inbound_call_date

    ,NULLIF(MAX(IF(direction='in', call_start_date,'1900-01-01')),'1900-01-01')                                                           AS last_inbound_call_date
    ,NULLIF(MAX(IF(direction='in' AND call_status = 'handled', call_start_date ,'1900-01-01')),'1900-01-01')                              AS last_handled_inbound_call_date

    ,NULLIF(MIN(IF(direction='out', call_start_date,'2100-01-01')),'2100-01-01')                                                          AS first_outbound_call_date
    ,NULLIF(MIN(IF(direction='out' AND call_status = 'handled', call_start_date,'2100-01-01')),'2100-01-01')                              AS first_handled_outbound_call_date
    ,NULLIF(MIN(IF(direction='out' AND call_status = 'handled' AND duree_comm >= 60, call_start_date,'2100-01-01')),'2100-01-01')         AS first_answered_outbound_call_date

    -- with datetime

    ,NULLIF(MIN(IF(direction='in', call_start ,'2100-01-01')),'2100-01-01')                                                               AS first_inbound_call_datetime
    ,NULLIF(MIN(IF(direction='in' AND call_status = 'handled', call_start,'2100-01-01')),'2100-01-01')                                    AS first_handled_inbound_call_datetime
    ,NULLIF(MIN(IF(direction='in' AND call_status = 'handled' AND duree_comm >= 60, call_start,'2100-01-01')),'2100-01-01')               AS first_answered_inbound_call_datetime

    ,NULLIF(MAX(IF(direction='in', call_start,'1900-01-01')),'1900-01-01')                                                                AS last_inbound_call_datetime
    ,NULLIF(MAX(IF(direction='in' AND call_status = 'handled', call_start ,'1900-01-01')),'1900-01-01')                                   AS last_handled_inbound_call_datetime

    ,NULLIF(MIN(IF(direction='out', call_start,'2100-01-01')),'2100-01-01')                                                               AS first_outbound_call_datetime
    ,NULLIF(MIN(IF(direction='out' AND call_status = 'handled', call_start,'2100-01-01')),'2100-01-01')                                   AS first_handled_outbound_call_datetime
    ,NULLIF(MIN(IF(direction='out' AND call_status = 'handled' AND duree_comm > 60, call_start,'2100-01-01')),'2100-01-01')                AS first_answered_outbound_call_datetime

    -- END

    ,NULLIF(MIN(IF(duree_comm>180, call_start_date,'2100-01-01')),'2100-01-01')                         AS first_pitch_call
    ,NULLIF(MAX(IF(duree_comm>180, call_start_date,'1900-01-01')),'1900-01-01')                         AS last_pitch_call
    ,IFNULL(SUM(duree_comm),0)                                                                          AS total_comm
    ,IFNULL(SUM(duree_svi),0)                                                                           AS total_svi
    ,IFNULL(SUM(duree_wait),0)                                                                          AS total_wait
    ,IFNULL(SUM(duree_call),0)                                                                          AS total_call
    ,MAX(duree_comm)                                                                                    AS max_comm_duration
    ,SUM(processing_cost_v1)                                                                            AS processing_cost_v1
    ,IF(MAX(IF(pc_v1_status = 'estimated',1,0)) = 1,'estimated','real')                                   AS real_or_estimated_v1
    ,SUM(processing_cost_v2)                                                                            AS processing_cost_v2
    ,IF(MAX(IF(pc_v2_status = 'estimated',1,0)) = 1,'estimated','real')                                   AS real_or_estimated_v2
    ,SUM(processing_cost)                                                                               AS processing_cost_final
    ,IF(MAX(IF(pc_status = 'estimated',1,0)) = 1,'estimated','real')                                      AS real_or_estimated_final

FROM clean_calls_miss_attribution AS calls
GROUP BY 1,2
)



SELECT
    phone_data.*
    ,date_diff(first_pitch_call, sct_creation_date, day)                       AS days_to_first_pitch_call
    ,date_diff(last_pitch_call, sct_creation_date, day)                        AS days_to_last_pitch_call
    ,date_diff(first_call_date, sct_creation_date, day)                        AS days_to_first_call
    ,date_diff(first_handled_call, sct_creation_date, day)                     AS days_to_first_handled_call
    ,date_diff(first_answered_date, sct_creation_date, day)                    AS days_to_first_answered_call
    ,IF(IFNULL(first_inbound_call_datetime,'1900-01-01') <= IFNULL(first_outbound_call_datetime,'1900-01-01'), 1, 0)                             AS is_first_call_inbound
    ,IF(IFNULL(first_handled_inbound_call_datetime,'1900-01-01') <= IFNULL(first_handled_outbound_call_datetime,'1900-01-01'), 1, 0)             AS is_first_handled_call_inbound
    ,IF(IFNULL(first_answered_inbound_call_datetime,'1900-01-01') <= IFNULL(first_answered_outbound_call_datetime,'1900-01-01'), 1, 0)           AS is_first_answered_call_inbound

    ,IF(nb_calls = 0, 1, 0)                                                    AS is_nevercontacted
    ,IF(max_comm_duration < 15, 1, 0)                                          AS is_nrp
    ,IF(max_comm_duration < 60, 1, 0)                                          AS is_uninterested
    ,IF(max_comm_duration < 180, 1, 0)                                         AS is_unqualified
    ,IF(max_comm_duration >= 180, 1, 0)                                        AS is_pitched
    ,CASE
        WHEN nb_calls = 0 THEN 'Never contacted'
        WHEN max_comm_duration < 15 THEN 'NRP'
        WHEN max_comm_duration < 60 THEN 'uninterested'
        WHEN max_comm_duration < 180 THEN 'unqualified'
        ELSE 'pitched' END                                                     AS lead_status_phone

FROM phone_data