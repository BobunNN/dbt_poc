-- datamart_prospect_contract
/* This table is an aggregation of dimension_contract on a prospect basis. 
To identify returning customers,who is client (net / raw ) AND to compute revenus on a segment basis (telecom,energy,insurance... 

*/


WITH 
	prep_contract AS(
        SELECT 
	        * 
        FROM 
	        {{ref('prep_contract')}}
        )

    ,prospect AS(
	    SELECT 
		    id_crm AS id_prospect
            ,sct_creation_date 
            ,IFNULL(no_contracts__c,'') AS rep_forbidden_contracts
        FROM 
            star_schema.dimension_crm 
        WHERE 
	        id_crm IS NOT NULL
        )

    ,prospectxcontract AS(
        SELECT
	        prep_contract.*
	        ,DATE_DIFF(opportunity_created_date,sct_creation_date,day) 			AS days_lead_opportunity
            ,rep_forbidden_contracts
        FROM 
	        prospect 
        LEFT JOIN 
	        prep_contract 
        USING 
	        (id_prospect)
    )

    ,pre_datamart_prospect_contract AS(
        SELECT
	        id_prospect
	        ,MIN(opportunity_created_date) 										   AS first_opportunity_date
	        ,MAX(opportunity_created_date)										   AS last_opportunity_date
	        ,MIN(net_date) 														   AS net_client_date
            ,MIN(brut_date)                                                        AS raw_client_date
            ,NULLIF(MIN(if(is_redirection=1,'2100-01-01',brut_date)),'2100-01-01') AS raw_client_wo_redirection_date
	        ,NULLIF(MIN(if(is_redirection=1,'2100-01-01',net_date)),'2100-01-01')  AS net_client_wo_redirection_date

	        ,IF(format_date('%Y',MIN(net_date))   = FORMAT_DATE('%Y',CURRENT_DATE),1,0) AS became_net_this_year
	        ,IF(format_date('%Y%m',MIN(net_date)) = FORMAT_DATE('%Y%m',CURRENT_DATE),1,0) AS became_net_this_month
	        ,IF(format_date('%Y%m',MIN(net_date)) IN (FORMAT_DATE('%Y%m',CURRENT_DATE), FORMAT_DATE('%Y%m', DATE_ADD(CURRENT_DATE,INTERVAL -1 MONTH))),1,0) AS became_net_last_two_months
	        ,IF(format_date('%Y%W',MIN(net_date)) = FORMAT_DATE('%Y%W',CURRENT_DATE),1,0) AS became_net_this_week

	        ,IF(SUM(is_net_this_year)>=1,1,0)                            		AS is_net_client_this_year
	        ,IF(SUM(is_net_this_month)>=1,1,0) 									AS is_net_client_this_month
	        ,IF(SUM(is_net_last_two_months)>=1,1,0) 							AS is_net_client_last_two_months
	        ,IF(SUM(is_net_this_week)>=1,1,0) 									AS is_net_client_this_week

	        ,IF(SUM(is_net_this_year*if(is_redirection=1,0,1))>=1,1,0)   		AS is_net_client_this_year_wo_redirection
	        ,IF(SUM(is_net_this_month*if(is_redirection=1,0,1))>=1,1,0)  		AS is_net_client_this_month_wo_redirection
	        ,IF(SUM(is_net_last_two_months*if(is_redirection=1,0,1))>=1,1,0)  	AS is_net_client_last_two_months_wo_redirection
	        ,IF(SUM(is_net_this_week*if(is_redirection=1,0,1))>=1,1,0)   		AS is_net_client_this_week_wo_redirection

	        ,IF(SUM(is_net_this_year*if(revenue_net>=10,1,0))>=1,1,0)    		AS is_net_client_this_year_with_remuneration
	        ,IF(SUM(is_net_this_month*if(revenue_net>=10,1,0))>=1,1,0)     		AS is_net_client_this_month_with_remuneration
	        ,IF(SUM(is_net_last_two_months*if(revenue_net>=10,1,0))>=1,1,0)     AS is_net_client_last_two_months_with_remuneration
	        ,IF(SUM(is_net_this_week*if(revenue_net>=10,1,0))>=1,1,0)      		AS is_net_client_this_week_with_remuneration

            #,SUM(is_net*isFullDemarche)										 AS nb_net_contracts_FullDemarche
            #,SUM(is_net*is_energy*isFullDemarche)								 AS nb_net_contracts_FullDemarche_energy
            #,SUM(is_net*is_box*isFullDemarche)								     AS nb_net_contracts_FullDemarche_box

	        ,SUM(is_net) 														AS nb_net_contracts
	        ,SUM(is_net_this_year) 												AS nb_net_contracts_this_year
	        ,SUM(is_net_this_month) 											AS nb_net_contracts_this_month
	        ,SUM(is_net_last_two_months) 										AS nb_net_contracts_last_two_months
	        ,SUM(is_net_this_week)												AS nb_net_contracts_this_week

	        ,SUM(is_net            * IF(is_redirection=1,0,1))					AS nb_net_contracts_wo_redirection								
	        ,SUM(is_net_this_year  * IF(is_redirection=1,0,1)) 					AS nb_net_contracts_this_year_wo_redirection
	        ,SUM(is_net_this_month * IF(is_redirection=1,0,1))					AS nb_net_contracts_this_month_wo_redirection
	        ,SUM(is_net_last_two_months * IF(is_redirection=1,0,1))				AS nb_net_contracts_last_two_months_wo_redirection
	        ,SUM(is_net_this_week  * IF(is_redirection=1,0,1)) 					AS nb_net_contracts_this_week_wo_redirection

	        ,SUM(is_net            *  IF(revenue_net >= 10,1,0)) 				AS nb_net_contracts_with_remuneration
	        ,SUM(is_net_this_year  *  IF(revenue_net >= 10,1,0)) 				AS nb_net_contracts_this_year_with_remuneration
	        ,SUM(is_net_this_month *  IF(revenue_net >= 10,1,0)) 			 	AS nb_net_contracts_this_month_with_remuneration
	        ,SUM(is_net_last_two_months *  IF(revenue_net >= 10,1,0)) 			AS nb_net_contracts_last_two_months_with_remuneration
	        ,SUM(is_net_this_week  *  IF(revenue_net >= 10,1,0)) 				AS nb_net_contracts_this_week_with_remuneration

	        ,SUM(is_brut) 														AS nb_brut_contracts
            ,SUM(is_dropped) 													AS nb_dropped_contracts
	        ,MAX(conversion_probability) 										AS client_e
	        ,SUM(conversion_probability)										AS contracts_e
	        ,SAFE_DIVIDE(SUM(conversion_probability), MAX(conversion_probability)) AS CS
	        ,MAX(IF(is_mortgage=1,conversion_probability,0))					AS client_e_mortgage
	        ,MAX(IF(is_redirection=1,0,conversion_probability)) 				AS client_e_wo_redirection

	        #,SUM(revenu_net*isFullDemarche)									 AS CM1_FullDemarche
	        #,SUM(revenu_e  *isFullDemarche)									 AS CM1_e_FullDemarche
	        ,SUM(revenue_e) 													AS CM1_e
	        ,SUM(revenue_net) 													AS CM1
	        ,SUM(revenue_net* IF(is_redirection=1,0,1))							AS CM1_adjusted

	        ,SUM(IF(days_lead_opportunity <= 90 ,is_net,0)) 					AS nb_net_contracts_90
	        ,SUM(IF(days_lead_opportunity <= 150,is_net,0))  					AS nb_net_contracts_150
	        ,SUM(IF(days_lead_opportunity >  90 ,is_net,0))    					AS nb_net_contracts_returning

	        ,SUM(IF(days_lead_opportunity <= 90 ,revenue_net,0))  				AS CM1_90
	        ,SUM(IF(days_lead_opportunity <= 150,revenue_net,0))				AS CM1_150
	        ,SUM(IF(days_lead_opportunity >  90 ,revenue_net,0))  				AS CM1_returning

	        ,SUM((is_brut+is_dropped+is_net) * is_insurance) 				    AS nb_brut_contracts_insurance
            ,IF(SUM((is_brut+is_dropped+is_net) * is_insurance) > 0, 1, 0)      AS has_brut_mrh
            ,MIN(IF((is_brut+is_dropped+is_net) * is_insurance = 1, brut_date, null)) AS date_first_brut_mrh
            ,SUM((is_brut+is_dropped+is_net) * is_insurance * IF(provider='papernest', 1, 0)) AS nb_brut_contracts_ppn_mrh
            ,IF(SUM((is_brut+is_dropped+is_net) * is_insurance * IF(provider='papernest', 1, 0)) > 0, 1, 0) AS has_brut_ppn_mrh
            ,MIN(IF((is_brut+is_dropped+is_net) * is_insurance * IF(provider='papernest', 1, 0) = 1, brut_date, null)) AS date_first_brut_ppn_mrh
	        ,SUM((is_brut+is_dropped+is_net) * is_affinity_insurance) 		    AS nb_brut_contracts_affinity_insurance
	        ,SUM((is_brut+is_dropped+is_net) * is_box) 						    AS nb_brut_contracts_box
	        ,SUM((is_brut+is_dropped+is_net) * is_mobile) 					    AS nb_brut_contracts_mobile
	        ,SUM((is_brut+is_dropped+is_net) * is_energy) 					    AS nb_brut_contracts_energy
	        ,SUM((is_brut+is_dropped+is_net) * is_redirection) 				    AS nb_brut_contracts_redirection
	        ,SUM((is_brut+is_dropped+is_net) * is_moving) 					    AS nb_brut_contracts_moving
	        ,SUM((is_brut+is_dropped+is_net) * is_mortgage) 				    AS nb_brut_contracts_mortgage
	        ,SUM((is_brut+is_dropped+is_net) * is_remotemonitoring) 		    AS nb_brut_contracts_remotemonitoring

	        ,IF(SUM(is_insurance)>0, 1, 0)										AS has_mrh_opportunity
	        ,IF(SUM(IF(provider = 'papernest', is_insurance,0))>0, 1, 0)		AS has_mrh_ppn_opportunity
	        ,SUM(is_net * is_insurance) 										AS nb_net_contracts_insurance
            ,IF(SUM(is_net * is_insurance) > 0, 1, 0)                           AS has_net_mrh
            ,MIN(IF(is_net * is_insurance = 1, net_date, null)) AS date_first_net_mrh
            ,SUM(is_net * is_insurance * IF(provider='papernest', 1, 0)) 		AS nb_net_contracts_ppn_insurance
            ,IF(SUM(is_net * is_insurance * IF(provider='papernest', 1, 0)) > 0, 1, 0)  AS has_net_ppn_mrh
            ,MIN(IF(is_net * is_insurance * IF(provider='papernest', 1, 0) = 1, net_date, null)) AS date_first_net_ppn_mrh
	        ,SUM(is_net * is_affinity_insurance) 								AS nb_net_contracts_affinity_insurance
	        ,SUM(is_net * is_box) 												AS nb_net_contracts_box
	        ,SUM(is_net * is_mobile) 											AS nb_net_contracts_mobile
	        ,SUM(is_net * is_energy) 											AS nb_net_contracts_energy
	        ,SUM(is_net * is_redirection) 										AS nb_net_contracts_redirection
	        ,SUM(is_net * is_moving) 											AS nb_net_contracts_moving
	        ,SUM(is_net * is_mortgage) 											AS nb_net_contracts_mortgage
	        ,SUM(is_net * is_remotemonitoring) 									AS nb_net_contracts_remotemonitoring
	        ,SUM(is_net * is_voltura) 											AS nb_net_contracts_voltura


            -- definition of payable contracts (for REP clients) according to Thomas Cherret, PO AppPro

            ,SUM(is_net * (
	        		 (is_insurance*IF(rep_forbidden_contracts NOT LIKE '%HousingInsurance%',1,0))
	        		+(is_mortgage*IF(rep_forbidden_contracts  NOT  LIKE '%Mortgage%',1,0))
	        		+((is_energy-is_voltura)*IF(rep_forbidden_contracts NOT LIKE '%Energy%',1,0))
	        		+(is_box*IF(rep_forbidden_contracts NOT LIKE '%InternetAccess%',1,0))
	        		+(is_mobile*IF(rep_forbidden_contracts NOT LIKE '%Mobile%',1,0))
	        		+(is_moving*IF(rep_forbidden_contracts NOT  LIKE  '%Moving%',1,0))
	        	--	+(is_mobility*if(rep_forbidden_contracts not LIKE '%Mobility%',1,0))
	        		)) AS nb_net_unwanted_contracts

	        ,SUM(is_net * is_payable)            								AS nb_net_contracts_payable
            ,SUM(is_net_this_year * is_payable)  								AS nb_net_contracts_payable_this_year
            ,SUM(is_net_this_month * is_payable) 								AS nb_net_contracts_payable_this_month
            ,SUM(is_net_last_two_months * is_payable) 							AS nb_net_contracts_payable_last_two_months
            ,SUM(is_net_this_week * is_payable) 								AS nb_net_contracts_payable_this_week
            ,NULLIF(
                MIN(IF(is_net * is_payable < 1, '2100-01-01',brut_date))
                ,'2100-01-01'
            ) AS became_payable_raw_client_date
            ,NULLIF(
                MIN(IF(is_net * is_payable < 1,'2100-01-01',net_date))
                ,'2100-01-01'
            ) AS became_payable_client_date

	        ,MAX(
                CASE WHEN 
	        	is_net * is_payable * (
	        		 (is_insurance*IF(rep_forbidden_contracts NOT LIKE '%HousingInsurance%',1,0))
	        		+((is_energy-is_voltura)*IF(rep_forbidden_contracts NOT LIKE '%Energy%',1,0))
	        		+(is_box*IF(rep_forbidden_contracts NOT LIKE '%InternetAccess%',1,0))
	        		+(is_mobile*IF(rep_forbidden_contracts NOT LIKE '%Mobile%',1,0))
	        		+(is_moving*IF(rep_forbidden_contracts NOT  LIKE  '%Moving%',1,0))
	        	--	+(is_mobility*if(rep_forbidden_contracts not LIKE '%Mobility%',1,0))
	        		)
               > 0 
	        THEN provider ELSE NULL END) AS net_payable_contract_provider_example

	        ,SUM(revenue_net * is_insurance) 									AS revenue_insurance
	        ,SUM(revenue_net * is_affinity_insurance) 							AS revenue_affinity_insurance
	        ,SUM(revenue_net * is_box) 											AS revenue_box
	        ,SUM(revenue_net * is_mobile) 										AS revenue_mobile
	        ,SUM(revenue_net * is_energy) 										AS revenue_energy
	        ,SUM(revenue_net * is_redirection) 									AS revenue_redirection
	        ,SUM(revenue_net * is_moving) 										AS revenue_moving
	        ,SUM(revenue_net * is_mortgage) 									AS revenue_mortgage
	        ,SUM(revenue_net * is_remotemonitoring) 							AS revenue_remotemonitoring

			,SUM(revenue_e * is_insurance) 										AS CM1_e_insurance
	        ,SUM(revenue_e * is_affinity_insurance) 							AS CM1_e_affinity_insurance
	        ,SUM(revenue_e * is_box) 											AS CM1_e_box
	        ,SUM(revenue_e * is_mobile) 										AS CM1_e_mobile
	        ,SUM(revenue_e * is_energy) 										AS CM1_e_energy
	        ,SUM(revenue_e * is_redirection) 									AS CM1_e_redirection
	        ,SUM(revenue_e * is_moving) 										AS CM1_e_moving
	        ,SUM(revenue_e * is_mortgage) 										AS CM1_e_mortgage
	        ,SUM(revenue_e * is_remotemonitoring) 								AS CM1_e_remotemonitoring

FROM
	prospectxcontract
GROUP BY 
	id_prospect
)

SELECT 
	 pre_datamart_prospect_contract.*
	,DATE_DIFF(last_opportunity_date,first_opportunity_date,day)		 AS day_first_last_opportunity
	,IF(nb_net_contracts >=1,1,0) 										 AS is_net_client
	,IF((nb_net_contracts - nb_net_contracts_redirection)>0,1,0) 		 AS is_net_client_wo_redirection
	,IF(nb_net_contracts_payable>0,1,0) 								 AS is_net_client_payable
	,IF(became_net_this_year*nb_net_contracts_payable_this_year>0,1,0) 	 AS became_net_client_payable_this_year
	,IF(became_net_this_month*nb_net_contracts_payable_this_month>0,1,0) AS became_net_client_payable_this_month
	,IF(became_net_last_two_months*nb_net_contracts_payable_last_two_months>0,1,0) AS became_net_client_payable_last_two_months
	,IF(became_net_this_week*nb_net_contracts_payable_this_week>0,1,0) 	AS became_net_client_payable_this_week
	,IF(nb_net_contracts_with_remuneration>0,1,0) 						AS is_net_client_with_remuneration
	,IF(client_e>0 AND client_e<1,1,0) 									AS is_raw_client
	,IF(client_e=0 AND nb_dropped_contracts>0,1,0) 						AS is_dropped_client
	,IF(CM1_90>0 AND CM1_returning>0,1,0) 							    AS is_returning_client
	,IF(CM1_90=0 AND CM1_returning>0,1,0) 							    AS is_late_activation
	,SAFE_DIVIDE(CM1_e, nb_net_contracts) 								AS PU

FROM 
	pre_datamart_prospect_contract