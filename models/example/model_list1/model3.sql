{{ config(materialized='table') }}

WITH drp AS ( --> Granularity = Prospect
    SELECT drp.*
      , dd.month_actual
      , dd.yyyymm
      , dd.year_actual
      , IF(lead_ownership='Tenant',1,0) AS is_tenant
      , IF(lead_ownership IN('OccupantHomeowner','Homeowner'),1,0) AS is_owner
      , IF(lead_ownership IS NULL,1,0) as is_ownership_unknown
      , IF(lead_ownership IS NOT NULL,1,0) as is_ownership_known
  FROM `bi_datamart.datamart_pro_prospect` AS drp
  JOIN `star_schema.dimension_date` AS dd
    ON drp.sct_creation_date = dd.date_actual 
  WHERE reliable
)

,drc AS(
  Select drc.id AS id_rep_company
    , drc.company_name AS company_name
    , drc.network_id
    , drc.network_name
    , drc.company_business_type as business_type
    , drc.business_type_network
    , drc.country 
    , drc.date_turned_contacted
    , drc.nb_locations
    , drc.nb_relocations
    , drc.nb_transactions
    , drc.nb_operations
    , drc.nb_inventories
    , location_ratio
    , transaction_ratio
    , COALESCE(drc.nb_locations,drc.nb_transactions,drc.nb_operations,drc.nb_inventories,drc.nb_relocations) AS has_market_data
FROM `bi_datamart.datamart_pro_company` AS drc
)
    ,seaso AS(
        SELECT
            CAST(MONTH AS int64) AS month_actual
            ,MAX(IF(seasonality_type='Apppro_location',seasonality_coeff,0)) AS seaso_loc
            ,MAX(IF(seasonality_type='Apppro_transaction',seasonality_coeff,0)) AS seaso_transac
            ,MAX(IF(seasonality_type='Apppro',seasonality_coeff,0)) AS seaso_global
        FROM
            team_sales_ops.fact_table_rep_seasonality
        WHERE country='France'
        GROUP BY 1
    )

    , table_year_month AS (
        SELECT DISTINCT 
            yyyymm 
            , if(date_trunc(current_date,month)=first_day_of_month,1,0) AS is_current_month 
            , if(date_trunc(date_sub(current_date,interval 1 month),month)=first_day_of_month,1,0) as is_last_month
            , month_actual
            , year_actual
            , first_day_of_month
            , last_day_of_month
            , safe_divide(date_diff(least(current_date,last_day_of_month),first_day_of_month,day),date_diff(last_day_of_month,first_day_of_month,day)) as runrate
            , DATE_DIFF(current_date(), last_day_of_month, MONTH)<12 AS in_twelve_rolling_months
            , DATE_DIFF(current_date(), last_day_of_month, MONTH)<24 AS in_twenty_four_rolling_months
            , seaso.seaso_loc AS seasonality_coeff_location
            , seaso.seaso_transac AS seasonality_coeff_transaction
            , seaso.seaso_global AS seasonality_coeff_all
        FROM `star_schema.dimension_date` 
        LEFT JOIN 
            seaso
        USING(month_actual)
        WHERE date_actual <= CURRENT_DATE() and date_actual>='2019-01-01' 
        Window 
        month   AS   (Partition by first_day_of_month)
        ,year   AS (Partition by first_day_of_year)
)

,drc_m as(
Select
  *  
  ,runrate*seasonality_coeff_location*if(coalesce(nb_locations,nb_relocations) is null,null,(ifnull(nb_locations,0)+ifnull(nb_relocations,0))) as rental_potential_monthly
  ,runrate*seasonality_coeff_transaction*nb_transactions as transaction_potential_monthly
  ,runrate*seasonality_coeff_all*nb_operations as operation_potential_monthly
  ,runrate/12*nb_inventories as inventories_potential_monthly
FROM
  drc
CROSS JOIN
  table_year_month

)
, payment AS ( --> Granularity = Agency
  SELECT id_rep_company
    , SUM(amount) AS recorded_amount_paid_total
    , SUM(IF(payment_type='Sponsoring',amount,0)) AS recorded_amount_paid_sponsoring
    , SUM(IF(payment_type='Special Offer',amount,0)) AS recorded_amount_paid_special_offer
    , SUM(IF(payment_type='Automatic Payment',amount,0)) AS recorded_amount_paid_automatic
    , SUM(IF(payment_type='Regularization',amount,0)) AS recorded_amount_paid_regularization
    , SUM(IF(payment_type<>'Automatic Payment',amount,0)) AS recorded_amount_paid_indirect -- all payment types except AP 
  FROM bi_datamart.datamart_app_pro_billing
  WHERE id_rep_company IS NOT NULL AND status IN ('paid')
    AND payed_at >= '2019-01-01'
    GROUP BY 1
)


/* The following table enables to compute the total number of leads starting from 2019. 
Afterwards,  we will be able to weight amounts such as AC_prospect, AC_SP, AC_SO, ... in function of the number of leads provided per month to get a monthly view about these amounts. */
, view_nb_leads_per_agency AS ( -- Granularity = Agency
  SELECT drp.id_rep_company
    , COUNT(drp.id_crm) AS nb_prospects_total
  FROM drp
  WHERE drp.sct_creation_date  >= '2019-01-01'
  GROUP BY 1
)

      
/* The following table puts a flag for all "reactivator prospects" i.e. prospect so that there are at least 3 other prospects during the 60 past days */ 
, reactivator_prospects AS ( --> Granularity = Agency x date_prospect
  SELECT drp.id_rep_company
    , drp.company_name 
    , drp.network_id
    , drp.network_name
    , drp.first_day_of_month
    , drp.sct_creation_date AS date_prospect
    , -- nested query
      (SELECT COUNT(drp_nested.id_crm) AS nb_prospects_last_60_days
       FROM drp AS drp_nested
       WHERE drp_nested.id_rep_company = drp.id_rep_company AND DATE_DIFF(drp.sct_creation_date, drp_nested.sct_creation_date, DAY) BETWEEN 0 AND 60
       ) AS nb_prospects_last_60_days
  FROM drp 
)

      
      
, reactivators_monthly_view AS ( --> Granularity = Agency x yyyymm
  SELECT react.id_rep_company 
    , react.company_name
    , react.network_id
    , react.network_name
    , react.first_day_of_month
    , COUNT(*) AS nb_possible_reactivators_this_month
    , MIN(react.date_reactivator_prospect) AS possible_reactivator_first_date
  FROM (
    SELECT react_prosp.* EXCEPT(date_prospect) 
      , react_prosp.date_prospect AS date_reactivator_prospect
    FROM reactivator_prospects AS react_prosp
    WHERE nb_prospects_last_60_days >= 4
   ) AS react -- react only contains reactivator prospects
  GROUP BY 1, 2, 3, 4, 5
)

, prospects_monthly_view AS ( --> Granularity = Agency x yyyymm
    SELECT 
    drc_m.id_rep_company
  , drc_m.company_name  
  , drc_m.country
  , drc_m.network_id
  , drc_m.network_name 
  , drc_m.date_turned_contacted
  , drc_m.is_current_month
  , drc_m.is_last_month
  , drc_m.month_actual
  , drc_m.year_actual
  , drc_m.first_day_of_month
  , drc_m.last_day_of_month
  , drc_m.business_type
  , drc_m.business_type_network
  
   
   /* Fields to display the 12 or 24 last months in charts */
  , ANY_VALUE(drc_m.in_twelve_rolling_months) AS in_twelve_rolling_months
  , ANY_VALUE(drc_m.in_twenty_four_rolling_months) AS in_twenty_four_rolling_months
  
  , COUNTIF(hasphonecalloptedin) AS nb_phone_call_optedin
  , COUNT(drp.id_crm) AS nb_prospects_this_month
   , -- nested query
      (SELECT STRUCT(MAX(drp_nested.sct_creation_date) as prospect_last_date_before_this_month
                    , COUNT(drp_nested.id_crm) AS nb_prospects_cum_before_this_month)
            FROM drp AS drp_nested
            WHERE drp_nested.id_rep_company = drc_m.id_rep_company AND drp_nested.sct_creation_date < drc_m.first_day_of_month
       ) cv
 

,rental_potential_monthly-sum(is_tenant)+round(sum(is_ownership_unknown*location_ratio),0) as rental_missing_potential
,transaction_potential_monthly-sum(is_owner)+round(sum(is_ownership_unknown*transaction_ratio),0) as transaction_missing_potential
,operation_potential_monthly-count(drp.id_crm) as operation_missing_potential
,inventories_potential_monthly-count(drp.id_crm) as inventories_missing_potential

,round(safe_divide(rental_potential_monthly,sum(is_tenant)+round(sum(is_ownership_unknown*location_ratio),0)),2) as rental_penetration
,round(safe_divide(transaction_potential_monthly,sum(is_owner)+round(sum(is_ownership_unknown*transaction_ratio),0)),2) as transaction_penetration
,round(safe_divide(operation_potential_monthly,count(drp.id_crm)),2) as operation_penetration
,round(safe_divide(inventories_potential_monthly,count(drp.id_crm)),2) as inventories_penetration


   , MIN(drp.sct_creation_date) AS date_first_prospect_of_the_month
   , COUNTIF(drp.presented) AS nb_presented_prospects
   , SUM(IFNULL(drp.is_net_client_wo_redirection, 0)) AS nb_net_clients
   , SUM(IFNULL( drp.client_e_wo_redirection, 0)) AS nb_client_e
   , SUM(drp.is_lead_payable) AS nb_leads_payable
   , SUM(drp.is_net_client_payable) AS nb_clients_payable
   , SUM(drp.nb_net_contracts_wo_redirection) AS nb_net_contracts
   , SUM(IF(drp.nb_net_contracts_wo_redirection=1, 1, 0)) AS nb_1_contract
   , SUM(IF(drp.nb_net_contracts_wo_redirection=2, 1, 0)) AS nb_2_contracts
   , SUM(IF(drp.nb_net_contracts_wo_redirection>2, 1, 0)) AS nb_3plus_contracts
   , SAFE_DIVIDE(SUM(IFNULL(drp.client_e_wo_redirection, 0)), COUNTIF(presented)) * 100 AS NTR
   
   , count(distinct drp.id_rep_user) as number_of_active_agents_this_month
   , safe_divide(count(drp.id_crm),count(distinct drp.id_rep_user)) as number_of_leads_per_active_agent
   
   /* Ownership infos */
   , sum(is_tenant)+round(sum(is_ownership_unknown*location_ratio),0) AS nb_tenants
   , sum(is_owner)+round(sum(is_ownership_unknown*transaction_ratio),0) AS nb_owners
   , SUM(is_tenant*CM1_adjusted) AS CM1_tenants
   , SUM(is_owner*CM1_adjusted)  AS CM1_owners
   , SUM(is_ownership_known*CM1_adjusted) AS CM1_ownership_known
   
   
   /* costs (AC & PC) & CM1 */
   , SUM(IFNULL(drp.processing_cost_final, 0)) AS PC
   , SUM(IFNULL(drp.theoritical_ac_amount_lead, 0)) AS AC_lead_theory
   , SUM(IFNULL(drp.theoritical_ac_amount_client, 0)) AS AC_client_theory
   , SUM(IFNULL(drp.theoritical_ac_amount_agency, 0)) AS AC_agency_theory
   , SUM(IFNULL(drp.theoritical_ac_amount_network, 0)) AS AC_network_theory
   , SUM(IFNULL(drp.theoritical_ac_amount_total, 0)) AS AC_total_theory
   , SUM(IFNULL(drp.lead_amount_paid_admin, 0)) AS AC_lead_recorded
   , SUM(IFNULL(drp.client_amount_paid_admin, 0)) AS AC_client_recorded
   , SUM(IFNULL(drp.total_paid_admin, 0)) AS AC_total_recorded
   
   , SUM(IFNULL(drp.CM1_adjusted, 0)) AS CM1
   , SUM(IFNULL(drp.CM1_e, 0)) AS CM1_e
   
 /* Acquisition costs - Breakdown by payment type */
  --  , payment.recorded_amount_paid_automatic AS AC_prospect_total_last_2_years => AC_total_recorded
    , ANY_VALUE(payment.recorded_amount_paid_indirect) AS AC_other_total_last_2_years
    , SAFE_DIVIDE(IFNULL(any_value(payment.recorded_amount_paid_automatic), 0)*COUNT(drp.id_crm), any_value(vla.nb_prospects_total)) AS AC_prospect_recorded -- = AC_AP_recorded
    
    , SAFE_DIVIDE(IFNULL(any_value(payment.recorded_amount_paid_indirect), 0)*COUNT(drp.id_crm), any_value(vla.nb_prospects_total)) AS AC_other_recorded
    , SAFE_DIVIDE(IFNULL(any_value(payment.recorded_amount_paid_special_offer), 0)*COUNT(drp.id_crm), any_value(vla.nb_prospects_total)) AS AC_SO_recorded
    , SAFE_DIVIDE(IFNULL(any_value(payment.recorded_amount_paid_sponsoring), 0)*COUNT(drp.id_crm), any_value(vla.nb_prospects_total)) AS AC_SP_recorded
    , SAFE_DIVIDE(IFNULL(any_value(payment.recorded_amount_paid_regularization), 0)*COUNT(drp.id_crm), any_value(vla.nb_prospects_total)) AS AC_R_recorded
   
   
FROM drc_m
LEFT JOIN drp 
  ON drc_m.id_rep_company = drp.id_rep_company
  AND drc_m.first_day_of_month = drp.first_day_of_month
LEFT JOIN payment
  ON drc_m.id_rep_company = payment.id_rep_company
LEFT JOIN view_nb_leads_per_agency AS vla
  ON drc_m.id_rep_company = vla.id_rep_company

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,rental_potential_monthly,transaction_potential_monthly,operation_potential_monthly,inventories_potential_monthly
ORDER BY 1 ASC, 4 ASC
)





, pmv_plus_churned_and_reactivators AS ( --> Granularity = Agency x yyyymm
  SELECT pmv.*
   , IFNULL(rmv.nb_possible_reactivators_this_month, 0) AS nb_possible_reactivators_this_month
   , rmv.possible_reactivator_first_date -- may be NULL
   , IF(
      ( (pmv.nb_prospects_this_month = 0 AND DATE_DIFF(pmv.last_day_of_month, cv.prospect_last_date_before_this_month, DAY) >= 90 + 1) 
        OR 
      (DATE_DIFF(date_first_prospect_of_the_month, pmv.cv.prospect_last_date_before_this_month, day) >= 90 + 1)
      )
    AND DATE_DIFF(pmv.first_day_of_month, pmv.cv.prospect_last_date_before_this_month, DAY) <= 90 -- was not churned before the 1st day of the month
      , DATE_ADD(pmv.cv.prospect_last_date_before_this_month, INTERVAL 91 day)
      , NULL
        )
            AS date_turned_churned_this_month -- possibly NULL
      ,rmv.possible_reactivator_first_date AS potential_reactivation_date
  FROM prospects_monthly_view AS pmv
  LEFT JOIN reactivators_monthly_view AS rmv
    ON pmv.id_rep_company = rmv.id_rep_company AND pmv.first_day_of_month = rmv.first_day_of_month
) 



, pmv_plus_info_from_past AS ( --> Granularity = Agency x yyyymm
  SELECT pmv.*
   , IFNULL(DATE_DIFF(pmv.first_day_of_month, pmv.cv.prospect_last_date_before_this_month, DAY) >= 90 + 1, FALSE)
            AS was_churned_or_dead_month_before
            
   , -- nested query for last date churned and last date reactivator prospect
    ( SELECT STRUCT(MAX(pmv_nested.date_turned_churned_this_month) as last_date_turned_churned_so_far
                    , MIN(pmv_nested.date_turned_churned_this_month) as first_date_turned_churned
                    , MAX(pmv_nested.possible_reactivator_first_date) AS last_date_possible_reactivator_so_far)
      FROM pmv_plus_churned_and_reactivators AS pmv_nested
      WHERE pmv_nested.id_rep_company = pmv.id_rep_company AND pmv_nested.first_day_of_month < pmv.first_day_of_month
    ) AS cv2 -- cumulated view
  FROM pmv_plus_churned_and_reactivators AS pmv
)




, pmv_plus_all_statuses AS (
SELECT 
  pmv.*  
 /* margin */
   , CM1_e - PC AS CM2
   , CM1_e - PC-AC_total_theory-AC_other_recorded AS CM3
   -- to be added : URSSAF computation  
  , IF(pmv.cv.nb_prospects_cum_before_this_month + pmv.nb_prospects_this_month >= 1, 1, 0) AS has_been_activated_so_far
  , IF(pmv.nb_prospects_this_month >= 1, 1, 0) AS is_active
  
  , CASE WHEN pmv.nb_prospects_this_month >= 1 AND pmv.nb_prospects_this_month <= 2 THEN "one_two_leads"
        WHEN pmv.nb_prospects_this_month >= 3 AND pmv.nb_prospects_this_month <= 4 THEN "three_four_leads"
        WHEN pmv.nb_prospects_this_month >= 5 AND pmv.nb_prospects_this_month <= 7 THEN "five_seven_leads"
        WHEN pmv.nb_prospects_this_month >= 8 THEN "eight_plus_leads" 
        ELSE "not_active" END
            AS is_active_details
  , IF(pmv.nb_prospects_this_month >= 1 AND pmv.nb_prospects_this_month <= 2, 1, 0) AS D_company
  , IF(pmv.nb_prospects_this_month >= 3 AND pmv.nb_prospects_this_month <= 4, 1, 0) AS C_company
  , IF(pmv.nb_prospects_this_month >= 5 AND pmv.nb_prospects_this_month <= 7, 1, 0) AS B_company
  , IF(pmv.nb_prospects_this_month >= 8, 1, 0) AS A_company
  
  , IF(pmv.nb_prospects_this_month = 0, 1, 0) AS is_inactive
  , IF(coalesce(date_turned_contacted, '2100-01-01')<=last_day_of_month OR pmv.cv.nb_prospects_cum_before_this_month + pmv.nb_prospects_this_month >=1,1,0) as is_contacted
  , IF(pmv.cv.nb_prospects_cum_before_this_month + pmv.nb_prospects_this_month = 1, 1, 0) AS is_activated_only
  , IF(pmv.cv.nb_prospects_cum_before_this_month + pmv.nb_prospects_this_month >= 1, 1, 0) AS is_activated
  , IF(pmv.cv.nb_prospects_cum_before_this_month + pmv.nb_prospects_this_month>= 2 AND pmv.cv.nb_prospects_cum_before_this_month + pmv.nb_prospects_this_month <= 5, 1, 0) AS is_farmed_only
  , IF(pmv.cv.nb_prospects_cum_before_this_month + pmv.nb_prospects_this_month>= 2 , 1, 0) AS is_farmed
  , IF(pmv.cv.nb_prospects_cum_before_this_month + pmv.nb_prospects_this_month >= 6, 1, 0) AS is_onboarded
/* 10 (Advanced) monthly agency status: they represent a partition */

  , CASE 
    WHEN pmv.nb_prospects_this_month = 0 
    THEN
        CASE 
          WHEN pmv.cv.nb_prospects_cum_before_this_month= 0 THEN "Not Activated"
          WHEN pmv.cv.nb_prospects_cum_before_this_month= 1 THEN "Not Farmed"
          WHEN DATE_DIFF(pmv.last_day_of_month, pmv.cv.prospect_last_date_before_this_month, DAY) BETWEEN 1  AND 30  THEN "Inactive_30"
          WHEN DATE_DIFF(pmv.last_day_of_month, pmv.cv.prospect_last_date_before_this_month, DAY) BETWEEN 31 AND 60  THEN "Inactive_60"
          WHEN DATE_DIFF(pmv.last_day_of_month, pmv.cv.prospect_last_date_before_this_month, DAY) BETWEEN 61 AND 90  THEN "Pre-pool"
        ELSE
          CASE  
            WHEN NOT(pmv.was_churned_or_dead_month_before) THEN "Churned" 
            ELSE "Dead" 
          END
        END
     ELSE -- nb_prospect_this_month >= 1
     CASE 
        WHEN pmv.cv.nb_prospects_cum_before_this_month = 0 THEN "Born"
        WHEN  pmv.cv2.last_date_turned_churned_so_far IS NOT NULL
              AND 
              (pmv.cv2.last_date_possible_reactivator_so_far IS NULL 
              OR  
               pmv.cv2.last_date_turned_churned_so_far >= pmv.cv2.last_date_possible_reactivator_so_far)
              AND 
              pmv.nb_possible_reactivators_this_month > 0
              THEN "Reactivated"
        WHEN pmv.was_churned_or_dead_month_before then "Revived"
        ELSE "Effective"
        END
    END as monthly_agency_status

FROM pmv_plus_info_from_past AS pmv
)


/* F I N A L   Q U E R Y  */ 
SELECT pmv.*
    ,  IF(monthly_agency_status="Not Activated", 1, 0) AS is_not_activated # status=0
    ,  IF(monthly_agency_status="Born", 1, 0) AS is_born # status=1
    ,  IF(monthly_agency_status='Not Farmed',1,0) AS is_not_farmed
    ,  IF(monthly_agency_status="Effective", 1, 0) AS is_effective # status=2
    ,  IF(monthly_agency_status="Revived", 1, 0) AS is_revived # status=3
    ,  IF(monthly_agency_status="Reactivated", 1, 0) AS is_re_activated # status=4
    ,  IF(monthly_agency_status="Inactive_30", 1, 0) AS is_inactive_30 # status=5
    ,  IF(monthly_agency_status="Inactive_60", 1, 0) AS is_inactive_60 # status=6
    ,  IF(monthly_agency_status="Pre-pool", 1, 0) AS is_pre_pool # status=7
    ,  IF(monthly_agency_status="Churned", 1, 0) AS is_churned # status=8the
    ,  IF(monthly_agency_status="Dead", 1, 0) AS is_dead # status=9
    
    , IF((pmv.cv2.first_date_turned_churned<=last_day_of_month and is_farmed=1) or monthly_agency_status="Churned",1,0) as has_churned_so_far
    , IF(was_churned_or_dead_month_before AND nb_prospects_this_month>=1 , 1, 0) AS is_retrieved
    , IF(was_churned_or_dead_month_before AND nb_prospects_this_month>=1 , date_first_prospect_of_the_month,NULL) AS retrieved_date
    , IF(monthly_agency_status="Born",date_first_prospect_of_the_month,NULL) AS birth_date
    , IF(monthly_agency_status="Revived",date_first_prospect_of_the_month,NULL) AS revived_date 
    , IF(monthly_agency_status="Reactivated",potential_reactivation_date,NULL) AS reactivation_date
FROM pmv_plus_all_statuses AS pmv
/* we eventually keep the two last years: */
WHERE pmv.first_day_of_month >= '2020-01-01'
