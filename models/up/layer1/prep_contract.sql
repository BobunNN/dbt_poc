{{ config(materialized='table') }}

/*
This table is a retreatment of star_schema.fact_table_contract to make it more business oriented
the main changes are:
    . removal of duplicates
    . contract_type and contract_status are splitten withing several columns to avoid having to deal with field with string_values
    . adding date for contract status changes ( 1/3 missing, mostly in 2017-2018)
*/

WITH 

    opportunity_changelog AS (
        SELECT 
        OpportunityId
        , NULLIF(MIN(CASE WHEN NewValue='BRUT' AND Field ='ContractValidation__c' THEN CreatedDate ELSE '2100-01-01' END), '2100-01-01' )     AS brut_date_cv
        , NULLIF(MIN(CASE WHEN NewValue='NET'  AND Field ='ContractValidation__c' THEN CreatedDate ELSE '2100-01-01' END), '2100-01-01' )     AS net_date_cv
        , NULLIF(MIN(CASE WHEN NewValue='BRUT' AND Field ='TECH_Validation__c'    THEN CreatedDate ELSE '2100-01-01' END), '2100-01-01' )     AS brut_date_tv
        , NULLIF(MIN(CASE WHEN NewValue='NET'  AND Field ='TECH_Validation__c'    THEN CreatedDate ELSE '2100-01-01' END), '2100-01-01' )     AS net_date_tv
        , MIN(IF(field='Owner' AND oldValue IN ('API App API App','0050Y0000031AJ5QAM') AND newValue IS NOT NULL,DATE(CreatedDate),NULL))     AS owner_set_on
        , MIN(IF(field='Reference__c' AND oldValue IS NULL AND newValue IS NOT NULL,DATE(CreatedDate),NULL))                                  AS reference_set_on
        , MIN(IF(field='AppStatus__c' AND newValue ='Validated in App',DATE(CreatedDate),NULL))                                               AS validated_in_app_set_on
        FROM (
            SELECT 
            OpportunityId
            , CAST(CreatedDate AS DATE) AS CreatedDate
            , Field
            , OldValue
            , newValue
            FROM 
                `souscritoo-1343.raw_airflow_tables.salesforce_opportunityfieldhistory` 
            WHERE 
                Field IN ('ContractValidation__c','TECH_Validation__c','Owner','Reference__c','AppStatus__c')  
        )
        
        GROUP BY OpportunityId
    )

    , enriched_opportunity_changelog AS(
        SELECT 
        sf_opport.id_sf AS OpportunityId
        , COALESCE(opportunity_changelog.brut_date_cv, opportunity_changelog.brut_date_tv, sf_opport.closedate)  AS brut_date
        , COALESCE(opportunity_changelog.net_date_cv, opportunity_changelog.net_date_tv, sf_opport.closedate)    AS net_date
        , IF(sf_opport.CreatedById = "0050Y0000031AJ5QAM", 1, 0) AS is_created_in_app
        , IF(opportunity_changelog.validated_in_app_set_on IS NOT NULL, 1, 0) AS is_validated_in_app
        , opportunity_changelog.owner_set_on
        , opportunity_changelog.reference_set_on
        , opportunity_changelog.validated_in_app_set_on
        , sf_opport.energycontract__c
        , sf_opport.recordtypeid
        , sf_contract.Offer__c
        FROM 
            `souscritoo-1343.star_schema.dimension_salesforce_opportunity` AS sf_opport      
        LEFT JOIN  
            opportunity_changelog
            ON opportunity_changelog.OpportunityId = sf_opport.id_sf
        LEFT JOIN 
            `souscritoo-1343.star_schema.dimension_salesforce_detailcontract` AS sf_contract
            ON sf_contract.id_sf = sf_opport.id_sf_detailcontract
    )

    , enriched_ftc AS(
        SELECT
        fct_cont.id_prospect
        , fct_cont.id_dim_sf_opportunity AS id_contract
        , dim_date.date_actual                                                                                          AS opportunity_created_date
        , FORMAT_DATE('%Y',dim_date.date_actual)                                                                        AS year
        , FORMAT_DATE('%m_%Y',dim_date.date_actual)                                                                     AS month_year
        , FORMAT_DATE('%W_%Y',dim_date.date_actual)                                                                     AS week_year
        , IF(fct_cont.contract_status = 'BRUT' OR (changlog.brut_date <= changlog.net_date AND fct_cont.contract_status in ('BRUT', 'NET')), changlog.brut_date, NULL)    AS brut_date  
        , IF(fct_cont.contract_status = 'NET', changlog.net_date, NULL)                                                 AS net_date
        , FORMAT_DATE('%Y',IF(fct_cont.contract_status = 'NET', changlog.net_date, NULL))                               AS net_year
        , FORMAT_DATE('%m_%Y',IF(fct_cont.contract_status = 'NET', changlog.net_date, NULL) )                           AS net_month_year
        , FORMAT_DATE('%W_%Y',IF(fct_cont.contract_status = 'NET', changlog.net_date, NULL) )                           AS net_week_year
        , fct_cont.contract_status
        , fct_cont.conversion_probability
        , LOWER(fct_cont.contract_type) AS contract_type
        , provider
        , is_fullDemarche AS isFullDemarche
        , fct_cont.prob_revenue
        , fct_cont.net_revenue
        , changlog.is_created_in_app
        , changlog.is_validated_in_app
        , changlog.owner_set_on
        , changlog.reference_set_on
        , changlog.validated_in_app_set_on
        , changlog.energycontract__c
        , changlog.Offer__c
        , changlog.recordtypeid
       
        FROM  
            `souscritoo-1343.star_schema.fact_table_contract` AS fct_cont
        LEFT JOIN  
            `souscritoo-1343.star_schema.dimension_date` AS dim_date
            ON id_dim_created_date = id_dim_date
        LEFT JOIN 
            enriched_opportunity_changelog AS changlog
            ON id_dim_sf_opportunity = OpportunityId
        WHERE  
            id_prospect IS NOT NULL AND id_dim_sf_opportunity IS NOT NULL 

    )

SELECT
    ftc.id_prospect
    , ftc.id_contract
    , opportunity_created_date
    , ftc.is_created_in_app
    , year
    , month_year
    , week_year
    , ftc.validated_in_app_set_on
    , ftc.is_validated_in_app
    , ftc.owner_set_on
    , ftc.reference_set_on
    , brut_date
    , net_date
    , IF(year = FORMAT_DATE('%Y',CURRENT_DATE),1,0)                       AS is_this_year
    , IF(month_year = FORMAT_DATE('%Y%m',CURRENT_DATE),1,0)               AS is_this_month
    , IF(month_year IN (FORMAT_DATE('%Y%m',CURRENT_DATE),FORMAT_DATE('%Y%m', DATE_ADD(CURRENT_DATE,INTERVAL -1 MONTH))),1,0) AS is_last_two_months
    , IF(week_year = FORMAT_DATE('%Y%m',CURRENT_DATE),1,0)                AS is_this_week
    , IF(net_year = FORMAT_DATE('%Y',CURRENT_DATE),1,0)                   AS is_net_this_year
    , CASE 
        WHEN ftc.contract_status IS NULL THEN opportunity_created_date
        WHEN ftc.contract_status='BRUT' THEN brut_date
        WHEN ftc.contract_status='NET' THEN net_date 
    END  AS last_status_reached_on
    , IF(net_month_year = FORMAT_DATE('%m_%Y',CURRENT_DATE),1,0)          AS is_net_this_month
    , IF(net_month_year IN (FORMAT_DATE('%m_%Y',CURRENT_DATE), FORMAT_DATE('%m_%Y', DATE_ADD(CURRENT_DATE,INTERVAL -1 MONTH))),1,0) AS is_net_last_two_months
    , IF(net_week_year = FORMAT_DATE('%W_%Y',CURRENT_DATE),1,0)           AS is_net_this_week
    , DATE_DIFF(brut_date,opportunity_created_date,day)                   AS day_to_brut
    , DATE_DIFF(net_date,opportunity_created_date,day)                    AS day_to_net
    , ftc.contract_status
    , ftc.provider
    , IF(ftc.isFullDemarche IS true,1,0)                                  AS isFullDemarche
    , IF(provider IS NULL,1,0)                                            AS is_provider_null 
    , IF(ftc.contract_status IS NULL,1,0)                                 AS is_status_null
    , IF(ftc.contract_status = 'BRUT' AND conversion_probability = 0,1,0) AS is_dropped
    , IF(ftc.contract_status = 'BRUT' AND conversion_probability > 0,1,0) AS is_brut
    , IF(ftc.contract_status = 'NET',1,0)                                 AS is_net   
-- Insurance and Affinity Insurrance
    , IF(contract_type LIKE '%insurance%' AND contract_type NOT LIKE '%affinity%',1,0) AS is_insurance 
    , IF(contract_type LIKE '%affinity%insurance%',1,0)                   AS is_affinity_insurance
    , IF(contract_type LIKE '%moving%',1,0)                               AS is_moving
    , IF(contract_type LIKE '%remote monitoring%',1,0)                    AS is_remotemonitoring 
    , IF(contract_type LIKE '%box%',1,0)                                  AS is_box
    , IF(contract_type LIKE '%mobile%',1,0)                               AS is_mobile 
    , IF(contract_type LIKE '%mail%',1,0)                                 AS is_redirection
    , IF(contract_type LIKE '%energy%',1,0)                               AS is_energy
    , IF(contract_type LIKE '%mortgage%',1,0)                             AS is_mortgage
-- list of contracts that won't trigger remuneration, to be updated
    , IF(provider IN ('Voltura'),1,0)                                     AS is_voltura
    , IF(
		recordtypeid IN
		( 
		'0120Y0000001FDmQAM' /*French Energy Opportunity*/
		, '0120Y0000001FDlQAM' /*French Box Opportunity*/
		, '0120Y000000yVWJQA2' /*French Mobile Opportunity*/
		, '0120Y0000001FDnQAM' /*French Insurance Opportunity*/
		, '0121i0000000MXNAA2' /*French Mortgage Opportunity*/
		, '0121i0000000NCfAAM' /*French Moving Opportunity*/
		, '0121i0000000LvhAAE' /*Spanish Box Opportunity*/
		, '0121i000000PdSKAA0' /*Spanish Mobile Opportunity*/
		, '0121i0000000MmrAAE' /*Italian Energy Opportunity*/
		, '0121i0000000NCGAA2' /*Italian Box Opportunity*/
		, '0121i0000000NCLAA2' /*Italian Mobile Opportunity*/
		, '0121i000000PiZuAAK' /*UK Energy Opportunity*/
		,'0121i000000PiZpAAK' /*UK Box Opportunity*/
		)
		,1,0)                                                             AS is_payable
    , ftc.conversion_probability
    , ftc.prob_revenue                                                    AS revenue_e
    , ftc.net_revenue                                                     AS revenue_net
    , ftc.contract_type
    , ftc.energycontract__c
    , ftc.Offer__c    

-- if contract status is net but no revenue associated  or contract status is not net but there is a net revenue or provider is missing
    , IF((contract_status = 'NET' AND ((net_revenue IS NULL OR net_revenue =0)) OR (contract_status <> 'NET' AND net_revenue>0) OR provider IS NULL), 1, 0) AS has_issue
FROM  
    enriched_ftc AS ftc