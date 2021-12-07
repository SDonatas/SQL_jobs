WITH TEMP_planning_monthly_years_selector

	AS

		(SELECT min(year) as current_audit_year,
				    min(year) + 1 as next_audit_year
		 
     FROM `dummy.rep_dats_sheet.closed_periods` as closed_periods_tb
     
		 WHERE closed_periods_tb.is_closed = False),
     
     
reps_dates AS

    (SELECT reps_dates_tb.rep_id AS rep_id,
            reps_dates_tb.region as region,

            PARSE_TIMESTAMP("%Y-%m-%d", CASE WHEN MIN(reps_dates_tb.start_date) IS NULL
                                               OR MIN(reps_dates_tb.start_date) = ""
                                             THEN "1900-01-01"
                                             ELSE MIN(reps_dates_tb.start_date)
                                              END
                  
                  ) as start_date_ts,



            PARSE_TIMESTAMP("%Y-%m-%d", CASE WHEN MAX(reps_dates_tb.end_date) IS NULL
                                                    OR MAX(reps_dates_tb.end_date) = ""
                                               THEN "2050-01-01"
                                               ELSE MAX(reps_dates_tb.end_date)
                                          END

                  
                  
                  ) as end_date_ts,

       

                  FROM `dummy.rep_dats_sheet.reps_dates` as reps_dates_tb

                  WHERE reps_dates_tb.rep_id IS NOT NULL


           GROUP BY rep_id, region
           
           ),
           
  reps_dimension AS
  
    (SELECT reps_tb.id as rep_id
    
     
     FROM `dummy.dummy.reps` as reps_tb
     
     INNER JOIN reps_dates as reps_dates_tb
     
     ON reps_tb.id = reps_dates_tb.rep_id
        AND reps_dates_tb.region = 'US'
     
     
     LEFT JOIN `dummy.dummy.dim_calendar` as dim_calendar_start_tb
     
     ON reps_dates_tb.start_date_ts = dim_calendar_start_tb.date
     
     
     LEFT JOIN `dummy.dummy.dim_calendar` as dim_calendar_end_tb
     
     ON reps_dates_tb.end_date_ts = dim_calendar_end_tb.date
     
     
      WHERE (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
        BETWEEN dim_calendar_start_tb.audit_year AND dim_calendar_end_tb.audit_year
          
          
     GROUP BY rep_id
    
    
    ),
     
     
health AS

  (SELECT reps_health_tb.year as audit_year,
          1 as audit_quarter,
          1 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.january) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          1 as audit_quarter,
          2 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.february) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          1 as audit_quarter,
          3 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.march) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          2 as audit_quarter,
          4 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.april) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          2 as audit_quarter,
          5 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.may) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          2 as audit_quarter,
          6 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.june) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
   
   
   UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          3 as audit_quarter,
          7 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.july) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          3 as audit_quarter,
          8 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.august) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          3 as audit_quarter,
          9 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.september) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          4 as audit_quarter,
          10 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.october) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          4 as audit_quarter,
          11 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.november) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
UNION ALL
   
   
   SELECT reps_health_tb.year as audit_year,
          4 as audit_quarter,
          12 as audit_month,
          reps_health_tb.rep_id as rep_id,
          reps_health_tb.region as region,
          
          MAX(reps_health_tb.december) as value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_health` as reps_health_tb
   
   WHERE reps_health_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
    
    
  ),
     



sellX AS

  (SELECT reps_sellx_tb.year as audit_year,
          reps_sellx_tb.rep_id as rep_id,
          reps_sellx_tb.region as region,
          
          MAX(monthly_rep_supplies) AS monthly_rep_supplies,
          MAX(monthly_call_center) AS monthly_call_center,
          MAX(monthly_iphone) AS monthly_iphone,
          MAX(monthly_isr_salary) AS monthly_isr_salary,
          MAX(monthly_web_mktg) AS monthly_web_mktg
          
          
   
   FROM `dummy.rep_dats_sheet.reps_sellx` as reps_sellx_tb
   
   WHERE reps_sellx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
    
    
    ),



     

repsSharps AS

  (SELECT reps_sharps_tb.year as audit_year,
          reps_sharps_tb.rep_id as rep_id,
          reps_sharps_tb.region as region,
          reps_sharps_tb.sharps_monthly as sharps_monthly
   
   
   FROM `dummy.rep_dats_sheet.reps_sharps` as reps_sharps_tb
   ),

repsFedex AS

  (SELECT reps_fedex_tb.year as audit_year,
          reps_fedex_tb.rep_id as rep_id,
          reps_fedex_tb.region as region,
          
          MAX(reps_fedex_tb.monthly_cost) as monthly_cost
  
   FROM `dummy.rep_dats_sheet.reps_fedex` as reps_fedex_tb
   
   WHERE reps_fedex_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
   
   
   ),
     
     

repsTaxPlan AS

  (SELECT reps_tax_plan_tb.year as audit_year,
          reps_tax_plan_tb.rep_id as rep_id,
          reps_tax_plan_tb.region as region,
          max(reps_tax_plan_tb.tax_plan) as tax_plan
  
  
   FROM `dummy.rep_dats_sheet.reps_tax_plan` as reps_tax_plan_tb
   
   WHERE reps_tax_plan_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   
   GROUP BY audit_year,
            rep_id,
            region
  
  ),
     
     
repsDX AS

  (SELECT reps_dx_tb.year as audit_year,
          1 as audit_quarter,
          1 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.january) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          1 as audit_quarter,
          2 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.february) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          1 as audit_quarter,
          3 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.march) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          2 as audit_quarter,
          4 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.april) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          2 as audit_quarter,
          5 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.may) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
            
            
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          2 as audit_quarter,
          6 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.june) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          3 as audit_quarter,
          7 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.july) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          3 as audit_quarter,
          8 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.august) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          3 as audit_quarter,
          9 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.september) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
            
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          4 as audit_quarter,
          10 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.october) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          4 as audit_quarter,
          11 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.november) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
            
            
   
   UNION ALL
   
   SELECT reps_dx_tb.year as audit_year,
          4 as audit_quarter,
          12 as audit_month,
          reps_dx_tb.rep_id as rep_id,
          reps_dx_tb.region as region,
          
          max(reps_dx_tb.december) as dx_value
          
  
  
   FROM `dummy.rep_dats_sheet.reps_dx` as reps_dx_tb
   
   WHERE reps_dx_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
   
   GROUP BY audit_year,
            rep_id,
            region
   
 
   
   ),


repsBonus

AS

  (SELECT 
     reps_bonus_tb.year as audit_year,
     reps_bonus_tb.rep_id as rep_id,
     reps_bonus_tb.region as region,

     max(reps_bonus_tb.bonus_tier_0_threshold) as bonus_tier_0_threshold,
     max(reps_bonus_tb.bonus_tier_0_percent) as bonus_tier_0_percent,

     max(reps_bonus_tb.bonus_tier_1_threshold) as bonus_tier_1_threshold,
     max(reps_bonus_tb.bonus_tier_1_percent) as bonus_tier_1_percent,

     max(reps_bonus_tb.bonus_tier_2_threshold) as bonus_tier_2_threshold,
     max(reps_bonus_tb.bonus_tier_2_percent) as bonus_tier_2_percent,

     max(reps_bonus_tb.bonus_tier_3_threshold) as bonus_tier_3_threshold,
     max(reps_bonus_tb.bonus_tier_3_percent) as bonus_tier_3_percent

      FROM `dummy.rep_dats_sheet.reps_bonus` as reps_bonus_tb
      
      WHERE reps_bonus_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
      
      GROUP BY audit_year, rep_id, region
      
      ),



repsAvgMargin
 
 AS
 
 (SELECT reps_avg_margin_tb.year as audit_year,
         reps_avg_margin_tb.rep_id as rep_id,
         reps_avg_margin_tb.region as region,
         max(reps_avg_margin_tb.avg_margin_percentage) as avg_margin_percentage
         
 
 
  FROM `dummy.rep_dats_sheet.reps_avg_margin` as reps_avg_margin_tb
  
  
  WHERE reps_avg_margin_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
  
  ),
  
  
BCR 

AS

(SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        1 as audit_quarter,
        1 as audit_month,
        max(january_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region
          
          
          
UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        1 as audit_quarter,
        2 as audit_month,
        max(february_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region
          
          
UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        1 as audit_quarter,
        3 as audit_month,
        max(march_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region
          
          
UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        2 as audit_quarter,
        4 as audit_month,
        max(april_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region
          
          
UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        2 as audit_quarter,
        5 as audit_month,
        max(may_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region
          
          

UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        2 as audit_quarter,
        6 as audit_month,
        max(june_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region
          
          
          
UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        3 as audit_quarter,
        7 as audit_month,
        max(july_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region


UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        3 as audit_quarter,
        8 as audit_month,
        max(august_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region
          
          
UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        3 as audit_quarter,
        9 as audit_month,
        max(september_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region
          
          
UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        4 as audit_quarter,
        10 as audit_month,
        max(october_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region
          
          

UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        4 as audit_quarter,
        11 as audit_month,
        max(november_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region
          
          
UNION ALL


SELECT reps_bcr_tb.year as audit_year,
        reps_bcr_tb.rep_id as rep_id,
        reps_bcr_tb.region as region,
        
        4 as audit_quarter,
        12 as audit_month,
        max(december_value) as bcr_value
        

 FROM  `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
 
 WHERE reps_bcr_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 
 GROUP BY audit_year,
          rep_id,
          region

          
 
 ),


 basePay
 
 AS
 
 (SELECT  reps_base_pay_tb.year as audit_year,
          1 as audit_quarter,
          1 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.january_mid + reps_base_pay_tb.january_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           
           
  UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          1 as audit_quarter,
          2 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.february_mid + reps_base_pay_tb.february_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           
           
  UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          1 as audit_quarter,
          3 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.march_mid + reps_base_pay_tb.march_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           
           
  UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          2 as audit_quarter,
          4 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.april_mid + reps_base_pay_tb.april_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           
           
           
  UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          2 as audit_quarter,
          5 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.may_mid + reps_base_pay_tb.may_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           
           
  UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          2 as audit_quarter,
          6 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.june_mid + reps_base_pay_tb.june_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           
           
           
 UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          3 as audit_quarter,
          7 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.july_mid + reps_base_pay_tb.july_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           
           
UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          3 as audit_quarter,
          8 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.august_mid + reps_base_pay_tb.august_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           
           
UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          3 as audit_quarter,
          9 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.september_mid + reps_base_pay_tb.september_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           
           
UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          4 as audit_quarter,
          10 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.october_mid + reps_base_pay_tb.october_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           
           
           
UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          4 as audit_quarter,
          11 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.november_mid + reps_base_pay_tb.november_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
           


UNION ALL
 
 
  SELECT  reps_base_pay_tb.year as audit_year,
          4 as audit_quarter,
          12 as audit_month,
          reps_base_pay_tb.rep_id as rep_id,
          reps_base_pay_tb.region as region,
          max(reps_base_pay_tb.december_mid + reps_base_pay_tb.december_end) as base_pay
  
  FROM `dummy.rep_dats_sheet.reps_base_pay` AS reps_base_pay_tb
  
  WHERE reps_base_pay_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  GROUP BY audit_year,
           rep_id,
           region
             
  
  
  ),


 baseData
 
 AS

 (SELECT reps_plan_quota_tb.year as audit_year,
         1 as audit_quarter,
         1 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.january as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
      AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)
      
  /* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         1 as audit_quarter,
         1 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.january as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  /* Exceptions End */   
 
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         1 as audit_quarter,
         2 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.february as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)
  
  
    /* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         1 as audit_quarter,
         2 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.february as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  /* Exceptions End */   
  
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         1 as audit_quarter,
         3 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.march as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)
  
  
  /* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         1 as audit_quarter,
         3 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.march as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  /* Exceptions End */   
  
  
  
  
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         2 as audit_quarter,
         4 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.april as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)
  
  
  
  /* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         2 as audit_quarter,
         4 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.april as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  /* Exceptions End */     
  
  
  
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         2 as audit_quarter,
         5 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.may as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)
  
  
  /* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         2 as audit_quarter,
         5 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.may as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  /* Exceptions End */       
  
  
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         2 as audit_quarter,
         6 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.june as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)
  
  /* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         2 as audit_quarter,
         6 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.june as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  /* Exceptions End */    
  
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         3 as audit_quarter,
         7 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.july as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)


 /* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         3 as audit_quarter,
         7 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.july as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 /* Exceptions End */    


  
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         3 as audit_quarter,
         8 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.august as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)
  
  
 /* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         3 as audit_quarter,
         8 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.august as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 /* Exceptions End */    
 
  
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         3 as audit_quarter,
         9 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.september as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)

 /* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         3 as audit_quarter,
         9 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.september as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 /* Exceptions End */    

  
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         4 as audit_quarter,
         10 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.october as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)
  

 /* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         4 as audit_quarter,
         10 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.october as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 /* Exceptions End */    
 
  
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         4 as audit_quarter,
         11 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.november as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)


/* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         4 as audit_quarter,
         11 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.november as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 /* Exceptions End */    
 

   UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         4 as audit_quarter,
         12 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.december as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota` AS reps_plan_quota_tb
  
  WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
  
  AND reps_plan_quota_tb.rep_id NOT IN (SELECT rep_id FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` WHERE year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector) GROUP BY rep_id)
  
/* Exceptions Start */    
  UNION ALL
  
  SELECT reps_plan_quota_tb.year as audit_year,
         4 as audit_quarter,
         12 as audit_month,
         reps_plan_quota_tb.region as region,
         reps_plan_quota_tb.rep_id as rep_id,
         reps_plan_quota_tb.december as quota
         
   
  FROM `dummy.rep_dats_sheet.reps_plan_quota_forecast_adj` AS reps_plan_quota_tb
  
  WHERE reps_plan_quota_tb.year = (SELECT next_audit_year FROM TEMP_planning_monthly_years_selector)
 /* Exceptions End */    
  
  ),
  
  
  baseDataCalculated
  
  AS
  
  (SELECT baseData_tb.*,
          /* AMV = Quota (GM) / (1 - AVG Margin) */
          
          CASE WHEN repsAvgMargin_tb.avg_margin_percentage IS NULL THEN NULL
               WHEN (1 - repsAvgMargin_tb.avg_margin_percentage) = 0 THEN 0
               ELSE baseData_tb.quota / (1 - repsAvgMargin_tb.avg_margin_percentage)
               END as AMV,
          
          
          CASE WHEN repsAvgMargin_tb.avg_margin_percentage IS NULL THEN NULL
               WHEN (1 - repsAvgMargin_tb.avg_margin_percentage) = 0 THEN 0
               ELSE (baseData_tb.quota / (1 - repsAvgMargin_tb.avg_margin_percentage)) * repsAvgMargin_tb.avg_margin_percentage
               END AS spend,
               
               
         CASE WHEN repsAvgMargin_tb.avg_margin_percentage IS NULL THEN NULL
              WHEN (1 - repsAvgMargin_tb.avg_margin_percentage) = 0 THEN 0
              ELSE (baseData_tb.quota / (1 - repsAvgMargin_tb.avg_margin_percentage)) * 0.05
              END AS terms,
         
         CASE WHEN baseData_tb.quota IS NULL THEN NULL
              WHEN BCR_tb.bcr_value IS NULL THEN NULL
              ELSE baseData_tb.quota * BCR_tb.bcr_value
              END AS BComm,
         
         
         
         /* quarterly Bonus is at 100 percent assumption */
         CASE WHEN baseData_tb.quota IS NULL THEN NULL
              WHEN repsBonus_tb.bonus_tier_1_percent IS NULL THEN NULL
              ELSE baseData_tb.quota * repsBonus_tb.bonus_tier_1_percent
              END QBonus,
         
         0 as OComm,
         
         0 as OIncent,
         
         repsDX_tb.dx_value as dx_value,
         
         repsTaxPlan_tb.tax_plan as tax_plan,
         
         basePay_tb.base_pay as base_pay,
         
         /* Employer Tax (ERTax)  =  TODO: table with percentages per rep * (BPay + BComm + QBonus) */
         
         CASE WHEN basePay_tb.base_pay IS NULL THEN NULL
              WHEN baseData_tb.quota IS NULL THEN NULL
              WHEN BCR_tb.bcr_value IS NULL THEN NULL
              WHEN repsBonus_tb.bonus_tier_1_percent IS NULL THEN NULL
              WHEN repsTaxPlan_tb.tax_plan IS NULL THEN NULL
              ELSE
                  (basePay_tb.base_pay
                  + (baseData_tb.quota * BCR_tb.bcr_value)
                  + (baseData_tb.quota * repsBonus_tb.bonus_tier_1_percent)
                  ) * repsTaxPlan_tb.tax_plan
              END AS ERTax,
          
          /* FedEx Charges (FedEx) */
          
          repsFedex_tb.monthly_cost as FedEx,
          
          /* Sharps */
          repsSharps_tb.sharps_monthly as sharps_monthly,
          
          
         /* Melt Cost = 0.585% * AMV

            Assay Cost = 0.915% * AMV

            OTHER FORMULAS:

            IRA = 3% * (BPay + BComm + QBonus) */
            
            
         /* Melt cost */
         
         
         CASE WHEN repsAvgMargin_tb.avg_margin_percentage IS NULL THEN NULL
              WHEN (1 - repsAvgMargin_tb.avg_margin_percentage) = 0 THEN 0
              ELSE (0.00585 * (baseData_tb.quota / (1 - repsAvgMargin_tb.avg_margin_percentage)))
              END as melt_cost,
              
         CASE WHEN repsAvgMargin_tb.avg_margin_percentage IS NULL THEN NULL
              WHEN (1 - repsAvgMargin_tb.avg_margin_percentage) = 0 THEN 0
              ELSE (0.00915 * (baseData_tb.quota / (1 - repsAvgMargin_tb.avg_margin_percentage)))
              END as assay_cost,
              
         CASE WHEN basePay_tb.base_pay IS NULL THEN NULL
              WHEN baseData_tb.quota IS NULL THEN NULL
              WHEN BCR_tb.bcr_value IS NULL THEN NULL
              WHEN repsBonus_tb.bonus_tier_1_percent IS NULL THEN NULL
              ELSE 0.03 * (basePay_tb.base_pay
                 + (baseData_tb.quota * BCR_tb.bcr_value)
                 + (baseData_tb.quota * repsBonus_tb.bonus_tier_1_percent)
                 )
              END AS IRA,
              
         
         CASE WHEN sellX_tb.monthly_isr_salary IS NULL THEN NULL
              WHEN sellX_tb.monthly_rep_supplies IS NULL THEN NULL
              WHEN sellX_tb.monthly_web_mktg IS NULL THEN NULL
              WHEN sellX_tb.monthly_iphone IS NULL THEN NULL
              WHEN sellX_tb.monthly_call_center IS NULL THEN NULL
              WHEN health_tb.value IS NULL THEN NULL 
              WHEN basePay_tb.base_pay IS NULL THEN NULL
              WHEN baseData_tb.quota IS NULL THEN NULL
              WHEN BCR_tb.bcr_value IS NULL THEN NULL
              WHEN repsBonus_tb.bonus_tier_1_percent IS NULL THEN NULL
              ELSE
              
         sellX_tb.monthly_isr_salary
         + sellX_tb.monthly_rep_supplies
         + sellX_tb.monthly_web_mktg
         + sellX_tb.monthly_iphone
         + sellX_tb.monthly_call_center
         + health_tb.value
         + (0.03 * (basePay_tb.base_pay
                 + (baseData_tb.quota * BCR_tb.bcr_value)
                 + (baseData_tb.quota * repsBonus_tb.bonus_tier_1_percent)
                 )
           ) END AS SellX,
           
           
           
        /* Contrib */
        CASE WHEN baseData_tb.quota IS NULL THEN NULL
             WHEN sellX_tb.monthly_isr_salary IS NULL THEN NULL
             WHEN sellX_tb.monthly_rep_supplies IS NULL THEN NULL
             WHEN sellX_tb.monthly_web_mktg IS NULL THEN NULL
             WHEN sellX_tb.monthly_iphone IS NULL THEN NULL
             WHEN sellX_tb.monthly_call_center IS NULL THEN NULL
             WHEN health_tb.value IS NULL THEN NULL
             WHEN basePay_tb.base_pay IS NULL THEN NULL
             WHEN BCR_tb.bcr_value IS NULL THEN NULL
             WHEN repsBonus_tb.bonus_tier_1_percent IS NULL THEN NULL
             WHEN repsAvgMargin_tb.avg_margin_percentage IS NULL THEN NULL
             WHEN repsDX_tb.dx_value IS NULL THEN NULL
             WHEN repsTaxPlan_tb.tax_plan IS NULL THEN NULL
             WHEN repsFedex_tb.monthly_cost IS NULL THEN NULL
             WHEN repsSharps_tb.sharps_monthly IS NULL THEN NULL
             ELSE
             
             
        baseData_tb.quota
        
       /* Deduct Terms */
        - CASE WHEN (1 - repsAvgMargin_tb.avg_margin_percentage) = 0 THEN 0
               ELSE (baseData_tb.quota / (1 - repsAvgMargin_tb.avg_margin_percentage)) * 0.05
               END

       /* CASE WHEN (1 - repsAvgMargin_tb.avg_margin_percentage) = 0 THEN 0
               ELSE (0.00585 * (baseData_tb.quota / (1 - repsAvgMargin_tb.avg_margin_percentage)))
               END as melt_cost,
              
         CASE WHEN (1 - repsAvgMargin_tb.avg_margin_percentage) = 0 THEN 0
              ELSE (0.00915 * (baseData_tb.quota / (1 - repsAvgMargin_tb.avg_margin_percentage)))
              END as assay_cost */



       /* Deduct COGS */
       - (basePay_tb.base_pay
          + (baseData_tb.quota * BCR_tb.bcr_value)
          + (baseData_tb.quota * repsBonus_tb.bonus_tier_1_percent)
          /* Other Commission as zero placeholder */
          + 0
          /* OIncent as zero placeholde */
          + 0

          + repsDX_tb.dx_value

          /* ER Tax */
          + (
                 (basePay_tb.base_pay
                  + (baseData_tb.quota * BCR_tb.bcr_value)
                  + (baseData_tb.quota * repsBonus_tb.bonus_tier_1_percent)
                  ) * repsTaxPlan_tb.tax_plan
            )

          /* Fedex */
          + repsFedex_tb.monthly_cost

          /* Sharps */
          + repsSharps_tb.sharps_monthly

          /* Melt cost */
          + CASE WHEN (1 - repsAvgMargin_tb.avg_margin_percentage) = 0 THEN 0
                 ELSE (0.00585 * (baseData_tb.quota / (1 - repsAvgMargin_tb.avg_margin_percentage)))
            END

          /* Assay Cost */
          + CASE WHEN (1 - repsAvgMargin_tb.avg_margin_percentage) = 0 THEN 0
                 ELSE (0.00915 * (baseData_tb.quota / (1 - repsAvgMargin_tb.avg_margin_percentage)))
            END

       )


       /* Deduct Sellx */
        - (sellX_tb.monthly_isr_salary
         + sellX_tb.monthly_rep_supplies
         + sellX_tb.monthly_web_mktg
         + sellX_tb.monthly_iphone
         + sellX_tb.monthly_call_center
         + health_tb.value
         + (0.03 * (basePay_tb.base_pay
                 + (baseData_tb.quota * BCR_tb.bcr_value)
                 + (baseData_tb.quota * repsBonus_tb.bonus_tier_1_percent)
                 )
           )
           )
           
       /* Deduct Overheads Ohx */
       - 0
           
           END AS Contrib,
              
   
   CASE WHEN baseData_tb.rep_id IS NULL
   		  OR baseData_tb.quota IS NULL
	    THEN FALSE
		ELSE TRUE
		END AS isQuotaPopulated,

   CASE WHEN repsAvgMargin_tb.rep_id IS NULL THEN FALSE ELSE TRUE END isAvgMarginPopulated,
   
   CASE WHEN BCR_tb.rep_id IS NULL
			OR BCR_tb.bcr_value IS NULL
   THEN FALSE
   ELSE TRUE END isBcrPopulated,

   CASE WHEN repsBonus_tb.rep_id IS NULL
          OR repsBonus_tb.bonus_tier_1_percent IS NULL
        THEN FALSE ELSE TRUE END isBonusPopulated,
        
   CASE WHEN repsDX_tb.rep_id IS NULL THEN FALSE ELSE TRUE END isDxPopulated,
   CASE WHEN repsTaxPlan_tb.rep_id IS NULL THEN FALSE ELSE TRUE END isTaxPlanPopulated,
   CASE WHEN basePay_tb.rep_id IS NULL THEN FALSE ELSE TRUE END isBasePayPopulated,
   CASE WHEN repsFedex_tb.rep_id IS NULL THEN FALSE ELSE TRUE END isFedexPopulated,
   CASE WHEN repsSharps_tb.rep_id IS NULL THEN FALSE ELSE TRUE END isSharpsPopulated,
   
   CASE WHEN sellX_tb.rep_id IS NULL
             OR sellX_tb.monthly_isr_salary IS NULL
             OR sellX_tb.monthly_rep_supplies IS NULL
             OR sellX_tb.monthly_web_mktg IS NULL
             OR sellX_tb.monthly_iphone IS NULL
             OR sellX_tb.monthly_call_center IS NULL
        THEN FALSE ELSE TRUE END isSellxPopulated,
        
   CASE WHEN health_tb.rep_id IS NULL THEN FALSE ELSE TRUE END isHealthPopulated
 
   
   
   FROM reps_dimension as reps_dimension_tb
   
   INNER JOIN baseData as baseData_tb
   
   ON reps_dimension_tb.rep_id = baseData_tb.rep_id
   
   
   /* Reps Average Margin Data For Calculating Market Value from GM */
   LEFT JOIN repsAvgMargin as repsAvgMargin_tb
   ON baseData_tb.audit_year = repsAvgMargin_tb.audit_year
      AND baseData_tb.rep_id = repsAvgMargin_tb.rep_id
      AND baseData_tb.region = repsAvgMargin_tb.region

   LEFT JOIN BCR as BCR_tb
   ON baseData_tb.audit_year = BCR_tb.audit_year
      AND baseData_tb.audit_month = BCR_tb.audit_month
      AND baseData_tb.rep_id = BCR_tb.rep_id
      AND baseData_tb.region = BCR_tb.region
      
      
   LEFT JOIN repsBonus as repsBonus_tb
   ON baseData_tb.audit_year = repsBonus_tb.audit_year
   AND baseData_tb.rep_id = repsBonus_tb.rep_id
   AND baseData_tb.region = repsBonus_tb.region
   
   
   LEFT JOIN repsDX as repsDX_tb
   ON baseData_tb.audit_year = repsDX_tb.audit_year
    AND baseData_tb.audit_month = repsDX_tb.audit_month
    AND baseData_tb.region = repsDX_tb.region
    AND baseData_tb.rep_id = repsDX_tb.rep_id
    
    
   LEFT JOIN repsTaxPlan as repsTaxPlan_tb
   ON baseData_tb.audit_year = repsTaxPlan_tb.audit_year
     AND baseData_tb.rep_id = repsTaxPlan_tb.rep_id
     AND baseData_tb.region = repsTaxPlan_tb.region
     
     
   LEFT JOIN basePay as basePay_tb
   ON baseData_tb.audit_year = basePay_tb.audit_year
    AND baseData_tb.audit_month = basePay_tb.audit_month
    AND baseData_tb.rep_id = basePay_tb.rep_id
    AND baseData_tb.region = basePay_tb.region
    
    
   LEFT JOIN repsFedex as repsFedex_tb
    ON baseData_tb.audit_year = repsFedex_tb.audit_year
    AND baseData_tb.rep_id = repsFedex_tb.rep_id
    AND baseData_tb.region = repsFedex_tb.region
   
   
   LEFT JOIN repsSharps as repsSharps_tb
   ON baseData_tb.audit_year = repsSharps_tb.audit_year
    AND baseData_tb.rep_id = repsSharps_tb.rep_id
    AND baseData_tb.region = repsSharps_tb.region
    
    
   LEFT JOIN sellX as sellX_tb
   ON baseData_tb.audit_year = sellX_tb.audit_year
   AND baseData_tb.rep_id = sellX_tb.rep_id
   AND baseData_tb.region = sellX_tb.region
   
   LEFT JOIN health as health_tb
   ON baseData_tb.audit_year = health_tb.audit_year
   AND baseData_tb.audit_month = health_tb.audit_month
   AND baseData_tb.rep_id = health_tb.rep_id
   AND baseData_tb.region = health_tb.region
   
   )
  

 SELECT reps_tb.name as repName,
        baseDataCalculated_tb.*,
        
        /* 
          CASE WHEN isQuotaPopulated = TRUE


          isQuotaPopulated
          isAvgMarginPopulated
          isBcrPopulated
          isBonusPopulated
          isDxPopulated
          isTaxPlanPopulated
          isBasePayPopulated
          isFedexPopulated
          isSharpsPopulated

          isSellxPopulated

          isHealthPopulated
           */
        
        
        CASE WHEN baseDataCalculated_tb.isQuotaPopulated = TRUE
                 AND baseDataCalculated_tb.isQuotaPopulated = TRUE
                 AND baseDataCalculated_tb.isAvgMarginPopulated = TRUE
                 AND baseDataCalculated_tb.isBcrPopulated = TRUE
                 AND baseDataCalculated_tb.isBonusPopulated = TRUE
                 AND baseDataCalculated_tb.isDxPopulated = TRUE
                 AND baseDataCalculated_tb.isTaxPlanPopulated = TRUE
                 AND baseDataCalculated_tb.isBasePayPopulated = TRUE
                 AND baseDataCalculated_tb.isFedexPopulated = TRUE
                 AND baseDataCalculated_tb.isSharpsPopulated = TRUE
                 AND baseDataCalculated_tb.isSellxPopulated = TRUE
                 AND baseDataCalculated_tb.isHealthPopulated = TRUE
                 THEN TRUE
                 ELSE FALSE
                 END as isAllTestsPassed
        
        
   FROM baseDataCalculated as baseDataCalculated_tb
   LEFT JOIN `dummy.dummy.reps` as reps_tb
   ON baseDataCalculated_tb.rep_id = reps_tb.id