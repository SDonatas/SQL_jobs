CREATE OR REPLACE PROCEDURE `dummy.dummy.ETL_REPORT_planning_monthly`()


BEGIN

/* Date Selectors */
CREATE TEMP TABLE TEMP_planning_monthly_years_selector

AS

SELECT min(year) as current_audit_year,
	   min(year) + 1 as next_audit_year
FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
WHERE closed_periods_tb.is_closed = False;




CREATE TEMP TABLE TEMP_SL_combined

AS

WITH SL_audit AS

	/* COMM REPORT TABLE FOR AUDIT */
	(SELECT /* Dimensions */
			hash_audit_bk as hash_bk,
			rep_id as rep_id,
			year_num as audit_year,
			month_num as audit_month,
			quarter_num as audit_quarter,
			"audit" as calculation_type,
			"actual" as data_type,
			0 as forecast_type_id,
			

			/* Metrics */
			SUM(CASE WHEN reps_base_pay_value IS NULL THEN 0 ELSE reps_base_pay_value END) as base_pay,
			
			SUM(plan_quota_weekly) as plan_quota,
			
			SUM(offers_total_offer) as spend,
			
			0 as revenue,
			
			SUM(audit_wc_est_mkt_value_AG_settlement
				+ audit_wc_est_mkt_value_AU_settlement
				+ audit_wc_est_mkt_value_PD_settlement
				+ audit_wc_est_mkt_value_PT_settlement
				+ audit_wd_est_mkt_value_AG_settlement
				+ audit_wd_est_mkt_value_AU_settlement
				+ audit_wd_est_mkt_value_PD_settlement
				+ audit_wd_est_mkt_value_PT_settlement

				+ audit_yc_est_mkt_value_AG_settlement
				+ audit_yc_est_mkt_value_AU_settlement
				+ audit_yc_est_mkt_value_PD_settlement
				+ audit_yc_est_mkt_value_PT_settlement

				+ audit_yd_est_mkt_value_AG_settlement
				+ audit_yd_est_mkt_value_AU_settlement
				+ audit_yd_est_mkt_value_PD_settlement
				+ audit_yd_est_mkt_value_PT_settlement

				+ audit_yr_est_mkt_value_AG_settlement
				+ audit_yr_est_mkt_value_AU_settlement
				+ audit_yr_est_mkt_value_PD_settlement
				+ audit_yr_est_mkt_value_PT_settlement
				) as market_value,
			
			

			SUM(audit_wc_est_mkt_value_AG_settlement
				+ audit_wc_est_mkt_value_AU_settlement
				+ audit_wc_est_mkt_value_PD_settlement
				+ audit_wc_est_mkt_value_PT_settlement
				+ audit_wd_est_mkt_value_AG_settlement
				+ audit_wd_est_mkt_value_AU_settlement
				+ audit_wd_est_mkt_value_PD_settlement
				+ audit_wd_est_mkt_value_PT_settlement

				+ audit_yc_est_mkt_value_AG_settlement
				+ audit_yc_est_mkt_value_AU_settlement
				+ audit_yc_est_mkt_value_PD_settlement
				+ audit_yc_est_mkt_value_PT_settlement

				+ audit_yd_est_mkt_value_AG_settlement
				+ audit_yd_est_mkt_value_AU_settlement
				+ audit_yd_est_mkt_value_PD_settlement
				+ audit_yd_est_mkt_value_PT_settlement

				+ audit_yr_est_mkt_value_AG_settlement
				+ audit_yr_est_mkt_value_AU_settlement
				+ audit_yr_est_mkt_value_PD_settlement
				+ audit_yr_est_mkt_value_PT_settlement
				) - SUM(offers_total_offer) AS GM,


				(SUM(audit_wc_est_mkt_value_AG_settlement
				+ audit_wc_est_mkt_value_AU_settlement
				+ audit_wc_est_mkt_value_PD_settlement
				+ audit_wc_est_mkt_value_PT_settlement
				+ audit_wd_est_mkt_value_AG_settlement
				+ audit_wd_est_mkt_value_AU_settlement
				+ audit_wd_est_mkt_value_PD_settlement
				+ audit_wd_est_mkt_value_PT_settlement

				+ audit_yc_est_mkt_value_AG_settlement
				+ audit_yc_est_mkt_value_AU_settlement
				+ audit_yc_est_mkt_value_PD_settlement
				+ audit_yc_est_mkt_value_PT_settlement

				+ audit_yd_est_mkt_value_AG_settlement
				+ audit_yd_est_mkt_value_AU_settlement
				+ audit_yd_est_mkt_value_PD_settlement
				+ audit_yd_est_mkt_value_PT_settlement

				+ audit_yr_est_mkt_value_AG_settlement
				+ audit_yr_est_mkt_value_AU_settlement
				+ audit_yr_est_mkt_value_PD_settlement
				+ audit_yr_est_mkt_value_PT_settlement
				) - SUM(offers_total_offer))
				* AVG(reps_bcr_value) AS commission,



				SUM(reps_base_pay_value)
				+ (
				(SUM(audit_wc_est_mkt_value_AG_settlement
				+ audit_wc_est_mkt_value_AU_settlement
				+ audit_wc_est_mkt_value_PD_settlement
				+ audit_wc_est_mkt_value_PT_settlement
				+ audit_wd_est_mkt_value_AG_settlement
				+ audit_wd_est_mkt_value_AU_settlement
				+ audit_wd_est_mkt_value_PD_settlement
				+ audit_wd_est_mkt_value_PT_settlement

				+ audit_yc_est_mkt_value_AG_settlement
				+ audit_yc_est_mkt_value_AU_settlement
				+ audit_yc_est_mkt_value_PD_settlement
				+ audit_yc_est_mkt_value_PT_settlement

				+ audit_yd_est_mkt_value_AG_settlement
				+ audit_yd_est_mkt_value_AU_settlement
				+ audit_yd_est_mkt_value_PD_settlement
				+ audit_yd_est_mkt_value_PT_settlement

				+ audit_yr_est_mkt_value_AG_settlement
				+ audit_yr_est_mkt_value_AU_settlement
				+ audit_yr_est_mkt_value_PD_settlement
				+ audit_yr_est_mkt_value_PT_settlement
				) - SUM(offers_total_offer))
				* AVG(reps_bcr_value)
				) as earnings,
				
				
				/* Quarterly bonus parameters for later calculation */
				AVG(quarterly_bonus_tier_0_percent) as quarterly_bonus_tier_0_percent,
				AVG(quarterly_bonus_tier_1_percent) as quarterly_bonus_tier_1_percent,
				AVG(quarterly_bonus_tier_2_percent) as quarterly_bonus_tier_2_percent,
				AVG(quarterly_bonus_tier_3_percent) as quarterly_bonus_tier_3_percent,
		
				AVG(quarterly_bonus_tier_0_threshold) as quarterly_bonus_tier_0_threshold,
				AVG(quarterly_bonus_tier_1_threshold) as quarterly_bonus_tier_1_threshold,
				AVG(quarterly_bonus_tier_2_threshold) as quarterly_bonus_tier_2_threshold,
				AVG(quarterly_bonus_tier_3_threshold) as quarterly_bonus_tier_3_threshold


	FROM `dummy.dummy.REPORT_spendLogAudit` as REPORT_spendLogAudit_tb

	/* WHERE year_num = 2021 and month_num = 1 */

	GROUP BY hash_bk,
			rep_id,
			audit_year,
			audit_month,
			audit_quarter,
			calculation_type,
			data_type),
					


	SL_assay AS

	(/* COMM REPORT TABLE FOR ASSAY */
	SELECT /* Dimensions */
			hash_assay_bk as hash_bk,
			rep_id as rep_id,
			year_num as audit_year,
			month_num as audit_month,
			quarter_num as audit_quarter,
			"assay" as calculation_type,
			"actual" as data_type,
			0 as forecast_type_id,

			/* Metrics */
			SUM(reps_base_pay_value) as base_pay,
			
			SUM(plan_quota_weekly) as plan_quota,
			
			SUM(offers_total_offer_assayed) as spend,
			
			SUM(rafinery_assay_pt_dollars)
			+ SUM(rafinery_assay_pd_dollars)
			+ SUM(rafinery_assay_au_dollars)
			+ SUM(rafinery_assay_ag_dollars) as revenue,
			
			SUM(rafinery_market_value_at_fix) as market_value,

			SUM(rafinery_market_value_at_fix) - SUM(offers_total_offer_assayed) AS GM,


			(SUM(rafinery_market_value_at_fix) - SUM(offers_total_offer_assayed))
			* AVG(reps_bcr_value) AS commission,


			SUM(reps_base_pay_value)
			+
			(
			(SUM(rafinery_market_value_at_fix) - SUM(offers_total_offer_assayed))
			* AVG(reps_bcr_value)
			) as earnings,
			
			/* Quarterly bonus parameters for later calculation */
			AVG(quarterly_bonus_tier_0_percent) as quarterly_bonus_tier_0_percent,
			AVG(quarterly_bonus_tier_1_percent) as quarterly_bonus_tier_1_percent,
			AVG(quarterly_bonus_tier_2_percent) as quarterly_bonus_tier_2_percent,
			AVG(quarterly_bonus_tier_3_percent) as quarterly_bonus_tier_3_percent,

			AVG(quarterly_bonus_tier_0_threshold) as quarterly_bonus_tier_0_threshold,
			AVG(quarterly_bonus_tier_1_threshold) as quarterly_bonus_tier_1_threshold,
			AVG(quarterly_bonus_tier_2_threshold) as quarterly_bonus_tier_2_threshold,
			AVG(quarterly_bonus_tier_3_threshold) as quarterly_bonus_tier_3_threshold



	FROM `dummy.dummy.REPORT_spendLogAudit` as REPORT_spendLogAudit_tb

	/* WHERE year_num = 2021 and month_num = 1 */

	GROUP BY hash_bk,
			rep_id,
			audit_year,
			audit_month,
			audit_quarter,
			calculation_type,
			data_type),
			
			

	SL_audit_enhanced AS

	(/* Audit */
	Select SL_audit_tb.hash_bk as hash_bk,
		SL_audit_tb.rep_id as rep_id,
		SL_audit_tb.audit_year as audit_year,
		SL_audit_tb.audit_month as audit_month,
		SL_audit_tb.audit_quarter as audit_quarter,
		SL_audit_tb.calculation_type as calculation_type,
		SL_audit_tb.data_type as data_type,
		SL_audit_tb.forecast_type_id as forecast_type_id,
		SL_audit_tb.base_pay as base_pay,
		SL_audit_tb.plan_quota as plan_quota,
		SL_audit_tb.market_value as market_value,
		SL_audit_tb.revenue as revenue,
		
		
		SL_audit_tb.GM as GM,
		SL_audit_tb.commission as commission,
		SL_audit_tb.earnings as earnings,
		
		CASE WHEN SL_audit_qtly_bonus_tb.qtly_GM IS NULL OR SL_audit_qtly_bonus_tb.qtly_GM = 0
				THEN 0
				ELSE (SL_audit_tb.GM / SL_audit_qtly_bonus_tb.qtly_GM) * SL_audit_qtly_bonus_tb.qtly_bonus
				END as qtly_bonus
				
		
		
		
	FROM SL_audit as SL_audit_tb

	LEFT JOIN
			(SELECT rep_id,
					audit_year,
					audit_quarter,
					
					CASE WHEN sum(plan_quota) = 0 OR sum(plan_quota) IS NULL 
						THEN 0
						ELSE 
						
							/* calculate Quarterly Bonus */
							CASE WHEN sum(market_value) / sum(plan_quota) < avg(quarterly_bonus_tier_0_threshold)
							
							THEN sum(GM) * avg(quarterly_bonus_tier_0_percent)
							
							WHEN sum(market_value) / sum(plan_quota) >= avg(quarterly_bonus_tier_0_threshold)
								AND sum(market_value) / sum(plan_quota) < avg(quarterly_bonus_tier_1_threshold)
							
							THEN 0
							
							WHEN sum(market_value) / sum(plan_quota) >= avg(quarterly_bonus_tier_1_threshold)
								AND sum(market_value) / sum(plan_quota) < avg(quarterly_bonus_tier_2_threshold)
								
							THEN sum(GM) * avg(quarterly_bonus_tier_1_percent)
							
							WHEN sum(market_value) / sum(plan_quota) >= avg(quarterly_bonus_tier_2_threshold)
								AND sum(market_value) / sum(plan_quota) < avg(quarterly_bonus_tier_3_threshold)
								
							THEN sum(GM) * avg(quarterly_bonus_tier_2_percent)
							
							WHEN sum(market_value) / sum(plan_quota) >= avg(quarterly_bonus_tier_3_threshold)
							
							THEN sum(GM) * avg(quarterly_bonus_tier_3_percent)
							
							ELSE NULL END
						
						END as qtly_bonus,
						
					sum(GM) as qtly_GM
						
						
					
			FROM SL_audit AS SL_audit_tb
			
			
			GROUP BY rep_id, audit_year, audit_quarter
			
			
			) AS SL_audit_qtly_bonus_tb
			

	ON SL_audit_tb.rep_id = SL_audit_qtly_bonus_tb.rep_id
	AND SL_audit_tb.audit_year = SL_audit_qtly_bonus_tb.audit_year
	AND SL_audit_tb.audit_quarter = SL_audit_qtly_bonus_tb.audit_quarter),
	
	
	
	SL_assay_enhanced AS


	(/* Assay */
	Select SL_assay_tb.hash_bk as hash_bk,
		SL_assay_tb.rep_id as rep_id,
		SL_assay_tb.audit_year as audit_year,
		SL_assay_tb.audit_month as audit_month,
		SL_assay_tb.audit_quarter as audit_quarter,
		SL_assay_tb.calculation_type as calculation_type,
		SL_assay_tb.data_type as data_type,
		SL_assay_tb.forecast_type_id as forecast_type_id,
		SL_assay_tb.base_pay as base_pay,
		SL_assay_tb.plan_quota as plan_quota,
		SL_assay_tb.market_value as market_value,
		SL_assay_tb.revenue as revenue,
		SL_assay_tb.GM as GM,
		SL_assay_tb.commission as commission,
		SL_assay_tb.earnings as earnings,
		

		CASE WHEN SL_assay_qtly_bonus_tb.qtly_GM IS NULL OR SL_assay_qtly_bonus_tb.qtly_GM = 0
				THEN 0
				ELSE (SL_assay_tb.GM / SL_assay_qtly_bonus_tb.qtly_GM) * SL_assay_qtly_bonus_tb.qtly_bonus
				END as qtly_bonus
				
		
		
		
	FROM SL_assay as SL_assay_tb

	LEFT JOIN
			(SELECT rep_id,
					audit_year,
					audit_quarter,
					
					CASE WHEN sum(plan_quota) = 0 OR sum(plan_quota) IS NULL 
						THEN NULL
						ELSE 
						
							/* calculate Quarterly Bonus */
							CASE WHEN sum(market_value) / sum(plan_quota) < avg(quarterly_bonus_tier_0_threshold)
							
							THEN sum(GM) * avg(quarterly_bonus_tier_0_percent)
							
							WHEN sum(market_value) / sum(plan_quota) >= avg(quarterly_bonus_tier_0_threshold)
								AND sum(market_value) / sum(plan_quota) < avg(quarterly_bonus_tier_1_threshold)
							
							THEN 0
							
							WHEN sum(market_value) / sum(plan_quota) >= avg(quarterly_bonus_tier_1_threshold)
								AND sum(market_value) / sum(plan_quota) < avg(quarterly_bonus_tier_2_threshold)
								
							THEN sum(GM) * avg(quarterly_bonus_tier_1_percent)
							
							WHEN sum(market_value) / sum(plan_quota) >= avg(quarterly_bonus_tier_2_threshold)
								AND sum(market_value) / sum(plan_quota) < avg(quarterly_bonus_tier_3_threshold)
								
							THEN sum(GM) * avg(quarterly_bonus_tier_2_percent)
							
							WHEN sum(market_value) / sum(plan_quota) >= avg(quarterly_bonus_tier_3_threshold)
							
							THEN sum(GM) * avg(quarterly_bonus_tier_3_percent)
							
							ELSE NULL END
						
						END as qtly_bonus,
						
						sum(GM) as qtly_GM
						
						
					
			FROM SL_assay AS SL_assay_tb
			
			
			GROUP BY rep_id, audit_year, audit_quarter
			
			
			) AS SL_assay_qtly_bonus_tb
			

	ON SL_assay_tb.rep_id = SL_assay_qtly_bonus_tb.rep_id
	AND SL_assay_tb.audit_year = SL_assay_qtly_bonus_tb.audit_year
	AND SL_assay_tb.audit_quarter = SL_assay_qtly_bonus_tb.audit_quarter),


	historic_data AS

		(SELECT TO_HEX(
				MD5(
				CONCAT(
						CASE WHEN REPORT_planning_monthly_historic_data_tb.rep_id IS NULL THEN "NULL" ELSE CAST(REPORT_planning_monthly_historic_data_tb.rep_id AS String) END,
						CASE WHEN REPORT_planning_monthly_historic_data_tb.audit_year IS NULL THEN "NULL" ELSE CAST(REPORT_planning_monthly_historic_data_tb.audit_year as String) END,
						CASE WHEN REPORT_planning_monthly_historic_data_tb.audit_month IS NULL THEN "NULL" ELSE CAST(REPORT_planning_monthly_historic_data_tb.audit_month as String) END,
						REPORT_planning_monthly_historic_data_tb.calculation_type
						)
				)
			) AS hash_bk,
			
		REPORT_planning_monthly_historic_data_tb.rep_id as rep_id,
		REPORT_planning_monthly_historic_data_tb.audit_year as audit_year,
		REPORT_planning_monthly_historic_data_tb.audit_month as audit_month,
		REPORT_planning_monthly_historic_data_tb.audit_quarter as audit_quarter,
		REPORT_planning_monthly_historic_data_tb.calculation_type as calculation_type,
		REPORT_planning_monthly_historic_data_tb.data_type as data_type,
		0 as forecast_type_id,
		REPORT_planning_monthly_historic_data_tb.base_pay as base_pay,
		REPORT_planning_monthly_historic_data_tb.quota as plan_quota,
		REPORT_planning_monthly_historic_data_tb.market_value as market_value,
		REPORT_planning_monthly_historic_data_tb.revenue as revenue,
		REPORT_planning_monthly_historic_data_tb.GM as GM,
		REPORT_planning_monthly_historic_data_tb.commission as commission,
		REPORT_planning_monthly_historic_data_tb.earnings as earnings,
		REPORT_planning_monthly_historic_data_tb.qtly_bonus as qtly_bonus
		

	FROM `dummy.dummy.REPORT_planning_monthly_historic_data` as REPORT_planning_monthly_historic_data_tb)
	
	

	/* historic data */ 
	SELECT * FROM historic_data WHERE hash_bk NOT IN (SELECT hash_bk FROM SL_audit_enhanced WHERE data_type = 'actual')
								AND hash_bk NOT IN (SELECT hash_bk FROM SL_assay_enhanced WHERE data_type = 'actual')
													
	
	UNION ALL
	
	/* current data */
	SELECT * FROM SL_audit_enhanced
	
	UNION ALL
	
	SELECT * FROM SL_assay_enhanced;


CREATE TEMP TABLE TEMP_FORECAST_year_on_year

AS

WITH Last_full_year_with_audit_and_assay AS

	(SELECT min(year) - 1 as current_audit_year
	 FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
	 WHERE closed_periods_tb.is_closed = False
         
   ),

	Year_on_year_change_full_percentages AS

		(SELECT outer_tb.audit_year as audit_year,
				outer_tb.calculation_type as calculation_type,
				outer_tb.rep_id as rep_id,
				
				CASE WHEN outer_tb.base_pay_lag IS NULL OR outer_tb.base_pay_lag = 0
					THEN 0
					ELSE outer_tb.base_pay / outer_tb.base_pay_lag
					END AS base_pay_yny_change_full_percent,
					
				CASE WHEN outer_tb.plan_quota_lag IS NULL OR outer_tb.plan_quota_lag = 0
					THEN 0
					ELSE outer_tb.plan_quota / outer_tb.plan_quota_lag
					END AS plan_quota_yny_change_full_percent,
					
				CASE WHEN outer_tb.market_value_lag IS NULL OR outer_tb.market_value_lag = 0
					THEN 0
					ELSE outer_tb.market_value / outer_tb.market_value_lag
					END AS market_value_yny_change_full_percent,
					
				CASE WHEN outer_tb.revenue_lag IS NULL OR outer_tb.revenue_lag = 0
					THEN 0
					ELSE outer_tb.revenue / outer_tb.revenue_lag
					END AS revenue_yny_change_full_percent,
					
					
				CASE WHEN outer_tb.GM_lag IS NULL OR outer_tb.GM_lag = 0
					THEN 0
					ELSE outer_tb.GM / outer_tb.GM_lag
					END AS GM_yny_change_full_percent,
					
				CASE WHEN outer_tb.commission_lag IS NULL OR outer_tb.commission_lag = 0
					THEN 0
					ELSE outer_tb.commission / outer_tb.commission_lag
					END AS commission_yny_change_full_percent,
					
					
				CASE WHEN outer_tb.earnings_lag IS NULL OR outer_tb.earnings_lag = 0
					THEN 0
					ELSE outer_tb.earnings / outer_tb.earnings_lag
					END AS earnings_yny_change_full_percent,
					
				CASE WHEN outer_tb.qtly_bonus_lag IS NULL OR outer_tb.qtly_bonus_lag = 0
					THEN 0
					ELSE outer_tb.qtly_bonus / outer_tb.qtly_bonus_lag
					END AS qtly_bonus_yny_change_full_percent
					
				
					
				
			FROM
			(SELECT inner_tb.*,
				LAG(base_pay) OVER (partition by calculation_type, rep_id order by calculation_type asc, rep_id asc, audit_year asc)
				as base_pay_lag,
				
				LAG(plan_quota) OVER (partition by calculation_type, rep_id order by calculation_type asc, rep_id asc, audit_year asc)
				as plan_quota_lag,
				
				LAG(market_value) OVER (partition by calculation_type, rep_id order by calculation_type asc, rep_id asc, audit_year asc)
				as market_value_lag,
				
				LAG(revenue) OVER (partition by calculation_type, rep_id order by calculation_type asc, rep_id asc, audit_year asc)
				as revenue_lag,
				
				LAG(GM) OVER (partition by calculation_type, rep_id order by calculation_type asc, rep_id asc, audit_year asc)
				as GM_lag,
				
				LAG(commission) OVER (partition by calculation_type, rep_id order by calculation_type asc, rep_id asc, audit_year asc)
				as commission_lag,
				
				LAG(earnings) OVER (partition by calculation_type, rep_id order by calculation_type asc, rep_id asc, audit_year asc)
				as earnings_lag,
				
				LAG(qtly_bonus) OVER (partition by calculation_type, rep_id order by calculation_type asc, rep_id asc, audit_year asc)
				as qtly_bonus_lag
				
				
				
				
				FROM
				(SELECT audit_year,
						calculation_type,
						rep_id,
						
						sum(base_pay) as base_pay,
						sum(plan_quota) as plan_quota,
						sum(market_value) as market_value,
						sum(revenue) as revenue,
						sum(GM) as GM,
						sum(commission) as commission,
						sum(earnings) as earnings,
						sum(qtly_bonus) as qtly_bonus
						
				FROM TEMP_SL_combined
				
				WHERE data_type = "actual"
					AND audit_year >= (SELECT audit_year - 1 FROM Last_full_year_with_audit_and_assay)
						
				GROUP BY audit_year,
						calculation_type,
						rep_id
						
						
				ORDER BY calculation_type ASC,
						audit_year ASC,
						rep_id ASC
						
						) as inner_tb
						
				
			) outer_tb
			
			
		
		)
	
	SELECT /* Dimensions */
		SL_combined_tb.hash_bk as hash_bk,
		SL_combined_tb.rep_id as rep_id,
		SL_combined_tb.audit_year + 1 as audit_year,
		SL_combined_tb.audit_month as audit_month,
		SL_combined_tb.audit_quarter as audit_quarter,
		SL_combined_tb.calculation_type as calculation_type,
		"forecast" as data_type,
		2 as forecast_type_id,
		
		/* Metrtics */
		SL_combined_tb.base_pay * Year_on_year_change_full_percentages_tb.base_pay_yny_change_full_percent as base_pay,
		SL_combined_tb.plan_quota * Year_on_year_change_full_percentages_tb.plan_quota_yny_change_full_percent  as plan_quota,
		SL_combined_tb.market_value * Year_on_year_change_full_percentages_tb.market_value_yny_change_full_percent  as market_value,
		SL_combined_tb.revenue * Year_on_year_change_full_percentages_tb.revenue_yny_change_full_percent as revenue,
		SL_combined_tb.GM * Year_on_year_change_full_percentages_tb.GM_yny_change_full_percent  as GM,
		SL_combined_tb.commission * Year_on_year_change_full_percentages_tb.commission_yny_change_full_percent  as commission,
		SL_combined_tb.earnings * Year_on_year_change_full_percentages_tb.earnings_yny_change_full_percent  as earnings,
		SL_combined_tb.qtly_bonus * Year_on_year_change_full_percentages_tb.qtly_bonus_yny_change_full_percent  as qtly_bonus
			
	

	 
	 FROM TEMP_SL_combined as SL_combined_tb
	
	 INNER JOIN Year_on_year_change_full_percentages as Year_on_year_change_full_percentages_tb
	 
	 
	 ON SL_combined_tb.audit_year = Year_on_year_change_full_percentages_tb.audit_year
	   AND SL_combined_tb.calculation_type = Year_on_year_change_full_percentages_tb.calculation_type
	    AND SL_combined_tb.rep_id = Year_on_year_change_full_percentages_tb.rep_id
	    
	
	 WHERE SL_combined_tb.data_type = "actual"
	      AND SL_combined_tb.audit_year >= (SELECT Last_full_year_with_audit_and_assay_tb.current_audit_year FROM Last_full_year_with_audit_and_assay as Last_full_year_with_audit_and_assay_tb)
	
	
	;


CREATE TEMP TABLE TEMP_monthly_planning_reps_bcr

AS

WITH reps_bcr_raw_tb AS

  (SELECT * 
   FROM `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_raw_tb
   WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
					AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   )
   

SELECT reps_bcr_tb.year as audit_year,
       1 as audit_quarter,
       1 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.january_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb

UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       1 as audit_quarter,
       2 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.february_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb

UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       1 as audit_quarter,
       3 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.march_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb

UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       2 as audit_quarter,
       4 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.april_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb


UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       2 as audit_quarter,
       5 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.may_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb


UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       2 as audit_quarter,
       6 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.june_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb


UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       3 as audit_quarter,
       7 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.july_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb


UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       3 as audit_quarter,
       8 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.august_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb

UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       3 as audit_quarter,
       9 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.september_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb

UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       4 as audit_quarter,
       10 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.october_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb

UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       4 as audit_quarter,
       11 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.november_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb

UNION ALL

SELECT reps_bcr_tb.year as audit_year,
       4 as audit_quarter,
       12 as audit_month,
       reps_bcr_tb.rep_id as rep_id,
       reps_bcr_tb.december_value as bcr_value
       
FROM reps_bcr_raw_tb as reps_bcr_tb;






CREATE TEMP TABLE TEMP_plan_quota_vw

AS

WITH current_audit_year_vw AS

	(SELECT max(year) as current_audit_year
	 FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
	 WHERE closed_periods_tb.is_closed = True
	)

SELECT reps_plan_quota_tb.year as year,
		reps_plan_quota_tb.rep_id as rep_id,
		reps_plan_quota_tb.january as january,
		reps_plan_quota_tb.february as february,
		reps_plan_quota_tb.march as march,
		reps_plan_quota_tb.april as april,
		reps_plan_quota_tb.may as may,
		reps_plan_quota_tb.june as june,
		reps_plan_quota_tb.july as july,
		reps_plan_quota_tb.august as august,
		reps_plan_quota_tb.september as september,
		reps_plan_quota_tb.october as october,
		reps_plan_quota_tb.november as november,
		reps_plan_quota_tb.december as december,

		reps_plan_quota_tb.year + 1 as year_increment_by_1
	 
	 FROM

	 `dummy.rep_dats_sheet.reps_plan_quota` as reps_plan_quota_tb

	 WHERE reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT current_audit_year FROM current_audit_year_vw) GROUP BY rep_id)

	
	     
	AND reps_plan_quota_tb.year = (SELECT current_audit_year FROM current_audit_year_vw)
	
	
	UNION ALL

	SELECT reps_plan_quota_forecast_adj_tb.year as year,
	        reps_plan_quota_forecast_adj_tb.rep_id as rep_id,
			reps_plan_quota_forecast_adj_tb.january as january,
			reps_plan_quota_forecast_adj_tb.february as february,
			reps_plan_quota_forecast_adj_tb.march as march,
			reps_plan_quota_forecast_adj_tb.april as april,
			reps_plan_quota_forecast_adj_tb.may as may,
			reps_plan_quota_forecast_adj_tb.june as june,
			reps_plan_quota_forecast_adj_tb.july as july,
			reps_plan_quota_forecast_adj_tb.august as august,
			reps_plan_quota_forecast_adj_tb.september as september,
			reps_plan_quota_forecast_adj_tb.october as october,
			reps_plan_quota_forecast_adj_tb.november as november,
			reps_plan_quota_forecast_adj_tb.december as december,
			reps_plan_quota_forecast_adj_tb.year + 1 as year_increment_by_1

	FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` as reps_plan_quota_forecast_adj_tb

	WHERE reps_plan_quota_forecast_adj_tb.year = (SELECT current_audit_year FROM current_audit_year_vw);




CREATE TEMP TABLE TEMP_plan_quota_vw_transformed

AS

SELECT
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			1 as audit_month,
			1 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.january as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.january as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb

	UNION ALL

	SELECT 
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			2 as audit_month,
			1 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.february as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.february as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb


	UNION ALL

	SELECT 
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			3 as audit_month,
			1 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.march as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.march as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb



	UNION ALL

	SELECT 
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			4 as audit_month,
			2 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.april as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.april as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb


	UNION ALL

	SELECT 
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			5 as audit_month,
			2 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.may as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.may as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb


	UNION ALL

	SELECT 
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			6 as audit_month,
			2 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.june as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.june as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb


	UNION ALL

	SELECT 
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			7 as audit_month,
			3 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.july as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.july as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb



	UNION ALL

	SELECT 
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			8 as audit_month,
			3 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.august as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.august as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb


	UNION ALL

	SELECT 
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			9 as audit_month,
			3 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.september as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.september as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb


	UNION ALL

	SELECT 
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			10 as audit_month,
			4 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.october as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.october as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb


	UNION ALL

	SELECT 
			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			11 as audit_month,
			4 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.november as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.november as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb


	UNION ALL

	SELECT 

			plan_quota_vw_tb.rep_id as rep_id,
			plan_quota_vw_tb.year as audit_year,
			12 as audit_month,
			4 as audit_quarter,
			"forecast" as data_type,
			3 as forecast_type_id,
			NULL as base_pay,
			plan_quota_vw_tb.december as plan_quota,
			NULL as market_value,
			NULL as revenue,
			plan_quota_vw_tb.december as GM,
			NULL as commission,
			NULL as earnings,
			NULL as qtly_bonus


	
	FROM TEMP_plan_quota_vw as plan_quota_vw_tb;



CREATE TEMP TABLE TEMP_planning_monthly_base_pay

AS

WITH current_audit_year_vw AS

	(SELECT max(year) as current_audit_year
	 FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
	 WHERE closed_periods_tb.is_closed = True
	)

     SELECT rep_id,
               year as audit_year,
			   1 as audit_quarter,
               1 as audit_month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "1")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "2")), INTERVAL 1 DAY) as end_date_ts, */
               january_mid + january_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
       



     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   1 as audit_quarter,
               2 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "2")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "3")), INTERVAL 1 DAY) as end_date_ts, */
               february_mid + february_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)

     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   1 as audit_quarter,
               3 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "3")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "4")), INTERVAL 1 DAY) as end_date_ts, */
               march_mid + march_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)

     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   2 as audit_quarter,
               4 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "4")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "5")), INTERVAL 1 DAY) as end_date_ts, */
               april_mid + april_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)

     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   2 as audit_quarter,
               5 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "5")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "6")), INTERVAL 1 DAY) as end_date_ts, */
               may_mid + may_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)

     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   2 as audit_quarter,
               6 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "6")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "7")), INTERVAL 1 DAY) as end_date_ts, */
               june_mid + june_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)


     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   3 as audit_quarter,
               7 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "7")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "8")), INTERVAL 1 DAY) as end_date_ts, */
               july_mid + july_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)

     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   3 as audit_quarter,
               8 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "8")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "9")), INTERVAL 1 DAY) as end_date_ts, */
               august_mid + august_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year = (SELECT current_audit_year FROM current_audit_year_vw)WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)

     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   3 as audit_quarter,
               9 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "9")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "10")), INTERVAL 1 DAY) as end_date_ts, */
               september_mid + september_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year = (SELECT current_audit_year FROM current_audit_year_vw)WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)

     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   4 as audit_quarter,
               10 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "10")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "11")), INTERVAL 1 DAY) as end_date_ts, */
               october_mid + october_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)


     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   4 as audit_quarter,
               11 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "11")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "12")), INTERVAL 1 DAY) as end_date_ts, */
               november_mid + november_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)

     UNION ALL

     SELECT rep_id,
               year as audit_year,
			   4 as audit_quarter,
               12 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "12")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year + 1 AS String), "1")), INTERVAL 1 DAY) as end_date_ts, */
               december_mid + december_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector);   


CREATE TEMP TABLE TEMP_REPORT_planning_monthly_bonus

AS

(SELECT reps_bonus_tb.year as audit_year,
 	    reps_bonus_tb.rep_id as rep_id,
		
		AVG(reps_bonus_tb.bonus_tier_0_threshold) as bonus_tier_0_threshold,
		AVG(reps_bonus_tb.bonus_tier_0_percent) as bonus_tier_0_percent,

		AVG(reps_bonus_tb.bonus_tier_1_threshold) as bonus_tier_1_threshold,
		AVG(reps_bonus_tb.bonus_tier_1_percent) as bonus_tier_1_percent,

		AVG(reps_bonus_tb.bonus_tier_2_threshold) as bonus_tier_2_threshold,
		AVG(reps_bonus_tb.bonus_tier_2_percent) as bonus_tier_2_percent,

		AVG(reps_bonus_tb.bonus_tier_3_threshold) as bonus_tier_3_threshold,
		AVG(reps_bonus_tb.bonus_tier_3_percent) as bonus_tier_3_percent


FROM `dummy.rep_dats_sheet.reps_bonus` as reps_bonus_tb

WHERE year BETWEEN (SELECT current_audit_year FROM TEMP_planning_monthly_years_selector)
	      AND (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)

GROUP BY audit_year,
		 rep_id

);






CREATE TEMP TABLE TEMP_REPORT_planning_monthly

AS
	
WITH Last_full_year_with_audit_and_assay AS

	(SELECT min(year) - 1 as current_audit_year
	 FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
	 WHERE closed_periods_tb.is_closed = False
         
   ),


current_audit_year_vw AS

	(SELECT min(year) as current_audit_year
	 FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
	 WHERE closed_periods_tb.is_closed = False
	),



current_audit_month_vw AS

    (SELECT min(month) as current_audit_month
	 FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
	 WHERE closed_periods_tb.year = (SELECT current_audit_year
									 FROM current_audit_year_vw
									)

	 AND closed_periods_tb.is_closed = False
			),

current_audit_quarter_vw AS

	(SELECT CASE WHEN current_audit_month BETWEEN 1 AND 3 THEN 1
				 WHEN current_audit_month BETWEEN 4 AND 6 THEN 2
	 			 WHEN current_audit_month BETWEEN 7 AND 9 THEN 3
				 WHEN current_audit_month BETWEEN 10 AND 12 THEN 4
		    END AS current_audit_quarter
	 
	 FROM current_audit_month_vw
	),


Dim_calendar AS 

	
	(SELECT inner_tb.audit_year as audit_year,
	       inner_tb.audit_quarter as audit_quarter,
	       inner_tb.audit_month as audit_month,
	       count(week_start_ts) as week_count
	
	
	FROM
	
	
	(SELECT audit_year,
	        audit_quarter,
	        audit_month,
	        week_start_ts
	
	 FROM `dummy.dummy.dim_calendar` as dim_calendar_tb
	
	WHERE audit_year >= (SELECT current_audit_year FROM Last_full_year_with_audit_and_assay)
	     AND audit_year <= (SELECT current_audit_year + 2 FROM Last_full_year_with_audit_and_assay)
	
	GROUP BY audit_year,
	         audit_quarter,
	         audit_month,
	         week_start_ts) as inner_tb
	
	GROUP BY audit_year,
	         audit_quarter,
	         audit_month),
	
	
Pre_Planning_reps_growth_rate_or_activity_level AS


	(SELECT planning_reps_tb.year - 1 as audit_year,
       planning_reps_tb.rep_id as rep_id,
       planning_reps_tb.region as region,
       planning_reps_tb.activity_level as activity_level,
       planning_reps_tb.growth_rate as growth_rate,
       


       (planning_level_activity_tb.plan_quota * planning_level_activity_seasonality_tb.q1) AS q1_level_activity,
		(reps_plan_quota_tb.january + reps_plan_quota_tb.february +  reps_plan_quota_tb.march) * (1 + planning_growth_rate_tb.q1) AS q1_growth_rate_fr_quota,
		planning_growth_rate_tb.q1 AS q1_growth_rate,
            
       (planning_level_activity_tb.plan_quota * planning_level_activity_seasonality_tb.q2) AS q2_level_activity,
	   (reps_plan_quota_tb.april + reps_plan_quota_tb.may +  reps_plan_quota_tb.june) * (1 + planning_growth_rate_tb.q2) AS q2_growth_rate_fr_quota,
	   planning_growth_rate_tb.q2 AS q2_growth_rate,
            
            
       (planning_level_activity_tb.plan_quota * planning_level_activity_seasonality_tb.q3) AS q3_level_activity,
	   (reps_plan_quota_tb.july + reps_plan_quota_tb.august +  reps_plan_quota_tb.september) * (1 + planning_growth_rate_tb.q3) AS q3_growth_rate_fr_quota,
	   planning_growth_rate_tb.q3 AS q3_growth_rate,
            
       (planning_level_activity_tb.plan_quota * planning_level_activity_seasonality_tb.q4) AS q4_level_activity,
	   (reps_plan_quota_tb.october + reps_plan_quota_tb.november +  reps_plan_quota_tb.december) * (1 + planning_growth_rate_tb.q4) AS q4_growth_rate_fr_quota,
	   planning_growth_rate_tb.q4 AS q4_growth_rate
            
       

	FROM `dummy.rep_dats_sheet.planning_reps` as planning_reps_tb
	
	/* Yearly figure */
	LEFT JOIN `dummy.rep_dats_sheet.planning_level_activity` as planning_level_activity_tb
	  ON planning_reps_tb.activity_level  = planning_level_activity_tb.activity_level 
	
	/* Seasonality figure spliting the early figure */
	LEFT JOIN `dummy.rep_dats_sheet.planning_level_activity_seasonality` as planning_level_activity_seasonality_tb
		ON planning_reps_tb.year = planning_level_activity_seasonality_tb.year
	  
	/* Planning growth rate per quarter */
	LEFT JOIN `dummy.rep_dats_sheet.planning_growth_rate` as planning_growth_rate_tb
	  ON planning_reps_tb.growth_rate = planning_growth_rate_tb.growth_assumption 

	LEFT JOIN TEMP_plan_quota_vw as reps_plan_quota_tb

	ON planning_reps_tb.rep_id = reps_plan_quota_tb.rep_id
	    AND planning_reps_tb.year = reps_plan_quota_tb.year_increment_by_1
	     
	WHERE planning_reps_tb.year = (SELECT current_audit_year_vw_tb.current_audit_year + 1 FROM current_audit_year_vw as current_audit_year_vw_tb)
		 
	      and planning_reps_tb.region = 'US'
	
	),


Planning_reps_growth_rate_or_activity_level AS

	(SELECT Pre_Planning_reps_growth_rate_or_activity_level_tb.audit_year as audit_year,
	        1 as audit_quarter,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.rep_id as rep_id,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.region as region,
			"audit" as calculation_type,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.activity_level as activity_level,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.growth_rate as growth_rate,


			Pre_Planning_reps_growth_rate_or_activity_level_tb.q1_level_activity as level_activity_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q1_growth_rate_fr_quota as growth_rate_fr_quota_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q1_growth_rate as growth_rate_value


	 FROM Pre_Planning_reps_growth_rate_or_activity_level as Pre_Planning_reps_growth_rate_or_activity_level_tb

	 UNION ALL

	 SELECT Pre_Planning_reps_growth_rate_or_activity_level_tb.audit_year as audit_year,
	        2 as audit_quarter,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.rep_id as rep_id,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.region as region,
			"audit" as calculation_type,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.activity_level as activity_level,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.growth_rate as growth_rate,


			Pre_Planning_reps_growth_rate_or_activity_level_tb.q2_level_activity as level_activity_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q2_growth_rate_fr_quota as growth_rate_fr_quota_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q2_growth_rate as growth_rate_value


	 FROM Pre_Planning_reps_growth_rate_or_activity_level as Pre_Planning_reps_growth_rate_or_activity_level_tb


	 UNION ALL

	 SELECT Pre_Planning_reps_growth_rate_or_activity_level_tb.audit_year as audit_year,
	        3 as audit_quarter,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.rep_id as rep_id,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.region as region,
			"audit" as calculation_type,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.activity_level as activity_level,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.growth_rate as growth_rate,


			Pre_Planning_reps_growth_rate_or_activity_level_tb.q3_level_activity as level_activity_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q3_growth_rate_fr_quota as growth_rate_fr_quota_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q3_growth_rate as growth_rate_value


	 FROM Pre_Planning_reps_growth_rate_or_activity_level as Pre_Planning_reps_growth_rate_or_activity_level_tb


	 UNION ALL

	 SELECT Pre_Planning_reps_growth_rate_or_activity_level_tb.audit_year as audit_year,
	        4 as audit_quarter,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.rep_id as rep_id,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.region as region,
			"audit" as calculation_type,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.activity_level as activity_level,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.growth_rate as growth_rate,


			Pre_Planning_reps_growth_rate_or_activity_level_tb.q4_level_activity as level_activity_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q4_growth_rate_fr_quota as growth_rate_fr_quota_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q4_growth_rate as growth_rate_value


	 FROM Pre_Planning_reps_growth_rate_or_activity_level as Pre_Planning_reps_growth_rate_or_activity_level_tb


	 
	 UNION ALL



	 SELECT Pre_Planning_reps_growth_rate_or_activity_level_tb.audit_year as audit_year,
	        1 as audit_quarter,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.rep_id as rep_id,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.region as region,
			"assay" as calculation_type,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.activity_level as activity_level,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.growth_rate as growth_rate,


			Pre_Planning_reps_growth_rate_or_activity_level_tb.q1_level_activity as level_activity_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q1_growth_rate_fr_quota as growth_rate_fr_quota_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q1_growth_rate as growth_rate_value


	 FROM Pre_Planning_reps_growth_rate_or_activity_level as Pre_Planning_reps_growth_rate_or_activity_level_tb

	 UNION ALL

	 SELECT Pre_Planning_reps_growth_rate_or_activity_level_tb.audit_year as audit_year,
	        2 as audit_quarter,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.rep_id as rep_id,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.region as region,
			"assay" as calculation_type,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.activity_level as activity_level,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.growth_rate as growth_rate,


			Pre_Planning_reps_growth_rate_or_activity_level_tb.q2_level_activity as level_activity_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q2_growth_rate_fr_quota as growth_rate_fr_quota_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q2_growth_rate as growth_rate_value


	 FROM Pre_Planning_reps_growth_rate_or_activity_level as Pre_Planning_reps_growth_rate_or_activity_level_tb


	 UNION ALL

	 SELECT Pre_Planning_reps_growth_rate_or_activity_level_tb.audit_year as audit_year,
	        3 as audit_quarter,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.rep_id as rep_id,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.region as region,
			"assay" as calculation_type,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.activity_level as activity_level,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.growth_rate as growth_rate,


			Pre_Planning_reps_growth_rate_or_activity_level_tb.q3_level_activity as level_activity_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q3_growth_rate_fr_quota as growth_rate_fr_quota_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q3_growth_rate as growth_rate_value


	 FROM Pre_Planning_reps_growth_rate_or_activity_level as Pre_Planning_reps_growth_rate_or_activity_level_tb


	 UNION ALL

	 SELECT Pre_Planning_reps_growth_rate_or_activity_level_tb.audit_year as audit_year,
	        4 as audit_quarter,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.rep_id as rep_id,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.region as region,
			"assay" as calculation_type,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.activity_level as activity_level,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.growth_rate as growth_rate,


			Pre_Planning_reps_growth_rate_or_activity_level_tb.q4_level_activity as level_activity_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q4_growth_rate_fr_quota as growth_rate_fr_quota_value,
			Pre_Planning_reps_growth_rate_or_activity_level_tb.q4_growth_rate as growth_rate_value


	 FROM Pre_Planning_reps_growth_rate_or_activity_level as Pre_Planning_reps_growth_rate_or_activity_level_tb

	
	 
	 ),
	
	
Forecast_reps_growth_rate_or_activity_level	AS


	(SELECT TO_HEX(
	              MD5(
	               CONCAT(
	                    CASE WHEN forecast_per_quarter_tb.rep_id IS NULL THEN "NULL" ELSE CAST(forecast_per_quarter_tb.rep_id AS String) END,
	                    CASE WHEN forecast_per_quarter_tb.audit_year IS NULL THEN "NULL" ELSE CAST(forecast_per_quarter_tb.audit_year as String) END,
	                    CASE WHEN dim_calendar_tb.audit_month IS NULL THEN "NULL" ELSE CAST(dim_calendar_tb.audit_month as String) END,
	                    forecast_per_quarter_tb.calculation_type
	                    )
	               )
	         ) AS hash_bk,
	         
	         forecast_per_quarter_tb.rep_id as rep_id,
	         forecast_per_quarter_tb.audit_year as audit_year,
	         dim_calendar_tb.audit_month as audit_month,
	         forecast_per_quarter_tb.audit_quarter as audit_quarter,
	         forecast_per_quarter_tb.calculation_type,
			 forecast_per_quarter_tb.data_type as data_type,
			 forecast_per_quarter_tb.forecast_type_id as forecast_type_id,
	         forecast_per_quarter_tb.base_pay,
	         forecast_per_quarter_tb.plan_quota * dim_calendar_tb.allocation_per_quarter_percetange as plan_quota,
	         forecast_per_quarter_tb.market_value as market_value,
	         forecast_per_quarter_tb.revenue as revenue,
	         forecast_per_quarter_tb.GM * dim_calendar_tb.allocation_per_quarter_percetange as GM,
	         forecast_per_quarter_tb.commission as commission,
	         forecast_per_quarter_tb.earnings as earnings,
	         forecast_per_quarter_tb.qtly_bonus as qtly_bonus
	         
	         
	
	
	
	FROM 
	
		(SELECT CASE WHEN actuals_tb.rep_id IS NOT NULL THEN actuals_tb.rep_id
					 ELSE Planning_reps_growth_rate_or_activity_level_tb.rep_id END as rep_id,

		        CASE WHEN actuals_tb.audit_year IS NOT NULL THEN actuals_tb.audit_year
				     ELSE Planning_reps_growth_rate_or_activity_level_tb.audit_year END + 1 as audit_year,

			    CASE WHEN actuals_tb.audit_quarter IS NOT NULL THEN actuals_tb.audit_quarter
				     ELSE Planning_reps_growth_rate_or_activity_level_tb.audit_quarter END as audit_quarter,

			    CASE WHEN actuals_tb.calculation_type IS NOT NULL THEN actuals_tb.calculation_type
				     ELSE Planning_reps_growth_rate_or_activity_level_tb.calculation_type
					 END as calculation_type,

			    "forecast" as data_type,

			    1 as forecast_type_id,
			   
			    NULL as base_pay,
			   
			    CASE WHEN Planning_reps_growth_rate_or_activity_level_tb.level_activity_value IS NOT NULL THEN Planning_reps_growth_rate_or_activity_level_tb.level_activity_value
					   		  WHEN current_audit_year_vw_tb.current_audit_year > actuals_tb.audit_year
							  THEN actuals_tb.GM * (1 + Planning_reps_growth_rate_or_activity_level_tb.growth_rate_value)

							  WHEN current_audit_year_vw_tb.current_audit_year = actuals_tb.audit_year
								 	AND current_audit_quarter_vw_tb.current_audit_quarter > actuals_tb.audit_quarter
									
							  THEN actuals_tb.GM * (1 + Planning_reps_growth_rate_or_activity_level_tb.growth_rate_value)

							  ELSE Planning_reps_growth_rate_or_activity_level_tb.growth_rate_fr_quota_value
							  END as plan_quota,
			   	    
			  /*
			   	    
			  CASE WHEN actuals_tb.audit_quarter = 1
			   	    THEN actuals_tb.market_value * Planning_reps_growth_rate_or_activity_level_tb.q1
			   	    
			   	    WHEN actuals_tb.audit_quarter = 2
			   	    THEN actuals_tb.market_value * Planning_reps_growth_rate_or_activity_level_tb.q2
			   	    
			   	    WHEN actuals_tb.audit_quarter = 3
			   	    THEN actuals_tb.market_value * Planning_reps_growth_rate_or_activity_level_tb.q3
			   	    
			   	    WHEN actuals_tb.audit_quarter = 4
			   	    THEN actuals_tb.market_value * Planning_reps_growth_rate_or_activity_level_tb.q4
			   	    
			   	    ELSE NULL END market_value, */
			   	    
			   NULL as market_value,
			   
			   /*
			   	    
			   	    
			   CASE WHEN actuals_tb.audit_quarter = 1
			   	    THEN actuals_tb.revenue * q1
			   	    
			   	    WHEN actuals_tb.audit_quarter = 2
			   	    THEN actuals_tb.revenue * q2
			   	    
			   	    WHEN actuals_tb.audit_quarter = 3
			   	    THEN actuals_tb.revenue * q3
			   	    
			   	    WHEN actuals_tb.audit_quarter = 4
			   	    THEN actuals_tb.revenue * q4
			   	    
			   	    ELSE NULL END revenue,
			   	    
			  */
			   
			   NULL as revenue,
			   	    
			 
			  CASE WHEN Planning_reps_growth_rate_or_activity_level_tb.level_activity_value IS NOT NULL THEN Planning_reps_growth_rate_or_activity_level_tb.level_activity_value
					   		  WHEN current_audit_year_vw_tb.current_audit_year > actuals_tb.audit_year
							  THEN actuals_tb.GM * (1 + Planning_reps_growth_rate_or_activity_level_tb.growth_rate_value)

							  WHEN current_audit_year_vw_tb.current_audit_year = actuals_tb.audit_year
								 	AND current_audit_quarter_vw_tb.current_audit_quarter > actuals_tb.audit_quarter
									
							  THEN actuals_tb.GM * (1 + Planning_reps_growth_rate_or_activity_level_tb.growth_rate_value)

							  ELSE Planning_reps_growth_rate_or_activity_level_tb.growth_rate_fr_quota_value
							  END as GM,
			   	    
			 /*  	    
			   	    
			 CASE WHEN actuals_tb.audit_quarter = 1
			   	    THEN actuals_tb.commission * q1
			   	    
			   	    WHEN actuals_tb.audit_quarter = 2
			   	    THEN actuals_tb.commission * q2
			   	    
			   	    WHEN actuals_tb.audit_quarter = 3
			   	    THEN actuals_tb.commission * q3
			   	    
			   	    WHEN actuals_tb.audit_quarter = 4
			   	    THEN actuals_tb.commission * q4
			   	    
			   	    ELSE NULL END commission,
			   	    
			 */
			   	    
			NULL as commission,
			
			/*
			   	    
			   	    
			 CASE WHEN actuals_tb.audit_quarter = 1
			   	    THEN actuals_tb.earnings * q1
			   	    
			   	    WHEN actuals_tb.audit_quarter = 2
			   	    THEN actuals_tb.earnings * q2
			   	    
			   	    WHEN actuals_tb.audit_quarter = 3
			   	    THEN actuals_tb.earnings * q3
			   	    
			   	    WHEN actuals_tb.audit_quarter = 4
			   	    THEN actuals_tb.earnings * q4
			   	    
			   	    ELSE NULL END earnings,
			 */
			
			NULL as earnings,
			  
		    /*
			   	    
			 CASE WHEN actuals_tb.audit_quarter = 1
			   	    THEN actuals_tb.qtly_bonus * q1
			   	    
			   	    WHEN actuals_tb.audit_quarter = 2
			   	    THEN actuals_tb.qtly_bonus * q2
			   	    
			   	    WHEN actuals_tb.audit_quarter = 3
			   	    THEN actuals_tb.qtly_bonus * q3
			   	    
			   	    WHEN actuals_tb.audit_quarter = 4
			   	    THEN actuals_tb.qtly_bonus * q4
			   	    
			   	    ELSE NULL END qtly_bonus
			*/
			
			NULL as qtly_bonus
			   
			
			FROM
			
			
			(SELECT audit_year,
			        audit_quarter,
			        calculation_type,
			        rep_id,
			        
			        sum(base_pay) as base_pay,
			        sum(plan_quota) as plan_quota,
			        sum(market_value) as market_value,
			        sum(revenue) as revenue,
			        sum(GM) as GM,
			        sum(commission) as commission,
			        sum(earnings) as earnings,
			        sum(qtly_bonus) as qtly_bonus
			        
			 FROM TEMP_SL_combined
			 
			 
			 WHERE data_type = "actual"
			       AND audit_year IN (SELECT audit_year FROM Planning_reps_growth_rate_or_activity_level GROUP BY audit_year)

			 GROUP BY audit_year,
			          audit_quarter,
			          calculation_type,
			          rep_id
			          
			          
			 ORDER BY audit_year ASC,
			          audit_quarter ASC,
			          calculation_type ASC,
			          rep_id ASC) as actuals_tb
			          
			
			 RIGHT JOIN Planning_reps_growth_rate_or_activity_level as Planning_reps_growth_rate_or_activity_level_tb
			 
			 ON actuals_tb.audit_year = Planning_reps_growth_rate_or_activity_level_tb.audit_year
			 	AND actuals_tb.rep_id = Planning_reps_growth_rate_or_activity_level_tb.rep_id
				 AND actuals_tb.audit_quarter = Planning_reps_growth_rate_or_activity_level_tb.audit_quarter
				 AND actuals_tb.calculation_type = Planning_reps_growth_rate_or_activity_level_tb.calculation_type
				 
			 CROSS JOIN current_audit_year_vw as current_audit_year_vw_tb

			 CROSS JOIN current_audit_quarter_vw as current_audit_quarter_vw_tb
				 
				 ) as forecast_per_quarter_tb
			 	
			 	
	INNER JOIN
	
	
		(SELECT dim_calendar_tb.audit_year as audit_year,
				dim_calendar_tb.audit_quarter as audit_quarter,
				dim_calendar_tb.audit_month as audit_month,
				dim_calendar_tb.week_count / dim_calendar_weeks_per_quarter_tb.total_week_count_per_quarter as allocation_per_quarter_percetange
				
				
		 
		  FROM Dim_calendar as dim_calendar_tb
		  
		  LEFT JOIN
		  
		  		(SELECT audit_year,
				        audit_quarter,
						sum(week_count) as total_week_count_per_quarter

		  		 FROM Dim_calendar

		  		 WHERE audit_year IN (SELECT audit_year + 1 FROM Planning_reps_growth_rate_or_activity_level GROUP BY audit_year)

		  		 GROUP BY audit_year, audit_quarter
		  		
		  		) as dim_calendar_weeks_per_quarter_tb
		  		
		  ON dim_calendar_tb.audit_year = dim_calendar_weeks_per_quarter_tb.audit_year
		  		AND dim_calendar_tb.audit_quarter = dim_calendar_weeks_per_quarter_tb.audit_quarter
		  
		  
		  WHERE dim_calendar_tb.audit_year IN (SELECT audit_year + 1 FROM Planning_reps_growth_rate_or_activity_level GROUP BY audit_year)
		  
		  
		  
		 ) as dim_calendar_tb
	 
	 
	 ON forecast_per_quarter_tb.audit_year = dim_calendar_tb.audit_year
	 	AND forecast_per_quarter_tb.audit_quarter = dim_calendar_tb.audit_quarter),




quota_fill_for_current_year AS

	(SELECT TO_HEX(
	              MD5(
	               CONCAT(
	                    CASE WHEN pre_quota_fill_for_current_year_tb.rep_id IS NULL THEN "NULL" ELSE CAST(pre_quota_fill_for_current_year_tb.rep_id AS String) END,
	                    CASE WHEN pre_quota_fill_for_current_year_tb.audit_year IS NULL THEN "NULL" ELSE CAST(pre_quota_fill_for_current_year_tb.audit_year as String) END,
	                    CASE WHEN pre_quota_fill_for_current_year_tb.audit_month IS NULL THEN "NULL" ELSE CAST(pre_quota_fill_for_current_year_tb.audit_month as String) END,
	                    "audit"
	                    )
	               )
	         ) AS hash_bk,
			pre_quota_fill_for_current_year_tb.rep_id as rep_id,
			pre_quota_fill_for_current_year_tb.audit_year as audit_year,
		    pre_quota_fill_for_current_year_tb.audit_month as audit_month,
			pre_quota_fill_for_current_year_tb.audit_quarter as audit_quarter,
			"audit" as calculation_type,
			pre_quota_fill_for_current_year_tb.data_type as data_type,
			pre_quota_fill_for_current_year_tb.forecast_type_id as forecast_type_id,
			TEMP_base_pay_tb.base_pay as base_pay,
			pre_quota_fill_for_current_year_tb.plan_quota as plan_quota,
			pre_quota_fill_for_current_year_tb.market_value as market_value,
			pre_quota_fill_for_current_year_tb.revenue as revenue,
			pre_quota_fill_for_current_year_tb.GM as GM,
			CASE WHEN pre_quota_fill_for_current_year_tb.GM IS NULL
			     THEN pre_quota_fill_for_current_year_tb.plan_quota * TEMP_monthly_planning_reps_bcr_tb.bcr_value
				 ELSE pre_quota_fill_for_current_year_tb.GM * TEMP_monthly_planning_reps_bcr_tb.bcr_value
				 END as commission,
			pre_quota_fill_for_current_year_tb.earnings as earnings,
			pre_quota_fill_for_current_year_tb.qtly_bonus as qtly_bonus



	 FROM TEMP_plan_quota_vw_transformed as pre_quota_fill_for_current_year_tb

	 LEFT JOIN TEMP_planning_monthly_base_pay as TEMP_base_pay_tb

	 ON pre_quota_fill_for_current_year_tb.audit_year = TEMP_base_pay_tb.audit_year
	   AND pre_quota_fill_for_current_year_tb.audit_quarter = TEMP_base_pay_tb.audit_quarter
	   AND pre_quota_fill_for_current_year_tb.audit_month = TEMP_base_pay_tb.audit_month
	   AND pre_quota_fill_for_current_year_tb.rep_id = TEMP_base_pay_tb.rep_id


	LEFT JOIN TEMP_monthly_planning_reps_bcr as TEMP_monthly_planning_reps_bcr_tb
		ON pre_quota_fill_for_current_year_tb.audit_year = TEMP_monthly_planning_reps_bcr_tb.audit_year
		  AND pre_quota_fill_for_current_year_tb.audit_quarter = TEMP_monthly_planning_reps_bcr_tb.audit_quarter
		  AND pre_quota_fill_for_current_year_tb.audit_month = TEMP_monthly_planning_reps_bcr_tb.audit_month
		  AND pre_quota_fill_for_current_year_tb.rep_id = TEMP_monthly_planning_reps_bcr_tb.rep_id

	 
	 UNION ALL 
	
	SELECT TO_HEX(
	              MD5(
	               CONCAT(
	                    CASE WHEN pre_quota_fill_for_current_year_tb.rep_id IS NULL THEN "NULL" ELSE CAST(pre_quota_fill_for_current_year_tb.rep_id AS String) END,
	                    CASE WHEN pre_quota_fill_for_current_year_tb.audit_year IS NULL THEN "NULL" ELSE CAST(pre_quota_fill_for_current_year_tb.audit_year as String) END,
	                    CASE WHEN pre_quota_fill_for_current_year_tb.audit_month IS NULL THEN "NULL" ELSE CAST(pre_quota_fill_for_current_year_tb.audit_month as String) END,
	                    "assay"
	                    )
	               )
	         ) AS hash_bk,
			pre_quota_fill_for_current_year_tb.rep_id as rep_id,
			pre_quota_fill_for_current_year_tb.audit_year as audit_year,
		    pre_quota_fill_for_current_year_tb.audit_month as audit_month,
			pre_quota_fill_for_current_year_tb.audit_quarter as audit_quarter,
			"assay" as calculation_type,
			pre_quota_fill_for_current_year_tb.data_type as data_type,
			pre_quota_fill_for_current_year_tb.forecast_type_id as forecast_type_id,
			TEMP_base_pay_tb.base_pay as base_pay,
			pre_quota_fill_for_current_year_tb.plan_quota as plan_quota,
			pre_quota_fill_for_current_year_tb.market_value as market_value,
			pre_quota_fill_for_current_year_tb.revenue as revenue,
			pre_quota_fill_for_current_year_tb.GM as GM,
			CASE WHEN pre_quota_fill_for_current_year_tb.GM IS NULL
			     THEN pre_quota_fill_for_current_year_tb.plan_quota * TEMP_monthly_planning_reps_bcr_tb.bcr_value
				 ELSE pre_quota_fill_for_current_year_tb.GM * TEMP_monthly_planning_reps_bcr_tb.bcr_value
				 END as commission,
			pre_quota_fill_for_current_year_tb.earnings as earnings,
			pre_quota_fill_for_current_year_tb.qtly_bonus as qtly_bonus



	 FROM TEMP_plan_quota_vw_transformed as pre_quota_fill_for_current_year_tb

	 LEFT JOIN TEMP_planning_monthly_base_pay as TEMP_base_pay_tb

	 ON pre_quota_fill_for_current_year_tb.audit_year = TEMP_base_pay_tb.audit_year
	   AND pre_quota_fill_for_current_year_tb.audit_quarter = TEMP_base_pay_tb.audit_quarter
	   AND pre_quota_fill_for_current_year_tb.audit_month = TEMP_base_pay_tb.audit_month
	   AND pre_quota_fill_for_current_year_tb.rep_id = TEMP_base_pay_tb.rep_id

	 
	 )
	          
	 

	
	
  
  
 
SELECT main_tb.*,
       reps_tb.name as repName,
       current_year_tb.current_audit_year as current_audit_year,
       CASE WHEN main_tb.audit_year = current_year_tb.current_audit_year
            THEN True
            ELSE False
            END AS is_current_audit_year,

       CASE WHEN main_tb.audit_year = current_year_tb.current_audit_year - 1
            THEN True
            ELSE False
            END AS is_previous_audit_year,

       CASE WHEN main_tb.audit_year = current_year_tb.current_audit_year - 2
            THEN True
            ELSE False
            END AS is_before_previous_audit_year,

       CASE WHEN main_tb.audit_year = current_year_tb.current_audit_year + 1
            THEN True
            ELSE False
            END AS is_next_audit_year,

	   
	   CASE WHEN main_tb.audit_year < current_year_tb.current_audit_year
	   		THEN True
			WHEN main_tb.audit_year = current_year_tb.current_audit_year
			THEN CASE WHEN main_tb.audit_month < current_month_tb.current_audit_month
					  THEN True
					  ELSE False
				  END
		    ELSE False
			END

	   AS is_closed_period


FROM

  (SELECT * FROM TEMP_SL_combined

  UNION ALL 

  SELECT * FROM TEMP_FORECAST_year_on_year
  
  UNION ALL
  
  SELECT * FROM Forecast_reps_growth_rate_or_activity_level

  UNION ALL

  SELECT * FROM quota_fill_for_current_year
  
  ) as main_tb

LEFT JOIN `dummy.dummy.reps` as reps_tb
  ON main_tb.rep_id = reps_tb.id


CROSS JOIN (SELECT max(year) as current_audit_year
			FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
			WHERE closed_periods_tb.is_closed = True
			) as current_year_tb


/* Last Complete Month and complete year from parameters sheet closed months and years */
CROSS JOIN (SELECT min(month) as current_audit_month
			FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
			WHERE closed_periods_tb.year = (SELECT max(year) as current_audit_year
											FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
											WHERE closed_periods_tb.is_closed = True)

			AND closed_periods_tb.is_closed = False
			) as current_month_tb;


/* Main report Loading */

IF (SELECT count(*) FROM dummy.__TABLES__ WHERE table_id='REPORT_planning_monthly') = 0 THEN
  CREATE TABLE IF NOT EXISTS `dummy.REPORT_planning_monthly`
  AS
	SELECT TEMP_REPORT_planning_monthly_tb.*

	FROM TEMP_REPORT_planning_monthly as TEMP_REPORT_planning_monthly_tb;

ELSE
    BEGIN
	/* DELETE OLD DATA */
	DELETE FROM `dummy.REPORT_planning_monthly`
	WHERE hash_bk IN (SELECT hash_bk FROM TEMP_REPORT_planning_monthly GROUP BY hash_bk);

	/* Load new data into a table */
	INSERT INTO `dummy.REPORT_planning_monthly`
	SELECT TEMP_REPORT_planning_monthly_tb.*,


	FROM TEMP_REPORT_planning_monthly AS TEMP_REPORT_planning_monthly_tb;
    END;
  
 END IF;

 END;