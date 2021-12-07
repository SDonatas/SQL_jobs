WITH reps_assessment_type AS

	(SELECT rep_id,
            TRIM(lower(max(commission_assessment))) as commission_assessment
      FROM `dummy.rep_dats_sheet.reps_dates`
      GROUP BY rep_id),


REPORT_planning_monthly AS

(SELECT REPORT_planning_monthly_tb.*

FROM `dummy.dummy.REPORT_planning_monthly` as REPORT_planning_monthly_tb
  
LEFT JOIN reps_assessment_type as reps_assessment_type_tb
  ON reps_assessment_type_tb.rep_id = REPORT_planning_monthly_tb.rep_id


WHERE (
	  /* Get Actuals */
      (REPORT_planning_monthly_tb.is_closed_period IS TRUE
      AND REPORT_planning_monthly_tb.data_type = 'actual')
      
      OR
      
       /* Get forecast for next year */
      (REPORT_planning_monthly_tb.is_closed_period IS FALSE
      AND REPORT_planning_monthly_tb.data_type = 'forecast' AND REPORT_planning_monthly_tb.forecast_type_id = 1)
  
      OR
  
      /* Get current year quota basepay  etc */
      (REPORT_planning_monthly_tb.is_closed_period IS FALSE
        AND REPORT_planning_monthly_tb.data_type = 'forecast'
        AND REPORT_planning_monthly_tb.forecast_type_id = 3)
      
      )
      
      
      AND
      
      (is_current_audit_year = True
       OR is_previous_audit_year = True
       OR is_before_previous_audit_year = True
       OR is_next_audit_year = True
      )
  
      AND reps_assessment_type_tb.commission_assessment = REPORT_planning_monthly_tb.calculation_type
 ),
  
reps_bcr AS
    
      (SELECT reps_bcr_tb.year as audit_year,
              reps_bcr_tb.rep_id as rep_id,
              AVG((CASE WHEN reps_bcr_tb.january_value IS NULL THEN 0 ELSE reps_bcr_tb.january_value END
                  + CASE WHEN reps_bcr_tb.february_value IS NULL THEN 0 ELSE reps_bcr_tb.february_value END
                  + CASE WHEN reps_bcr_tb.march_value IS NULL THEN 0 ELSE reps_bcr_tb.march_value END
                  + CASE WHEN reps_bcr_tb.april_value IS NULL THEN 0 ELSE reps_bcr_tb.april_value END
                  + CASE WHEN reps_bcr_tb.may_value IS NULL THEN 0 ELSE reps_bcr_tb.may_value END
                  + CASE WHEN reps_bcr_tb.june_value IS NULL THEN 0 ELSE reps_bcr_tb.june_value END
                  + CASE WHEN reps_bcr_tb.july_value IS NULL THEN 0 ELSE reps_bcr_tb.july_value END
                  + CASE WHEN reps_bcr_tb.august_value IS NULL THEN 0 ELSE reps_bcr_tb.august_value END
                  + CASE WHEN reps_bcr_tb.september_value IS NULL THEN 0 ELSE reps_bcr_tb.september_value END
                  + CASE WHEN reps_bcr_tb.october_value IS NULL THEN 0 ELSE reps_bcr_tb.october_value END
                  + CASE WHEN reps_bcr_tb.november_value IS NULL THEN 0 ELSE reps_bcr_tb.november_value END
                  + CASE WHEN reps_bcr_tb.december_value IS NULL THEN 0 ELSE reps_bcr_tb.december_value END) / 12
              ) as value
              
       FROM `dummy.rep_dats_sheet.reps_bcr` as reps_bcr_tb
      
       WHERE reps_bcr_tb.year BETWEEN (SELECT  min(audit_year) FROM REPORT_planning_monthly)
                                      AND (SELECT max(audit_year) FROM REPORT_planning_monthly)

       GROUP BY audit_year,
                rep_id

       )



 SELECT REPORT_planning_monthly_tb.*,
  	    CASE WHEN reps_bcr_tb.value IS NULL THEN 0 ELSE reps_bcr_tb.value END AS bcr_value,
        CASE WHEN REPORT_planning_monthly_tb.audit_month BETWEEN 1 AND 3 THEN CONCAT("Month ", CAST(REPORT_planning_monthly_tb.audit_month as String))
			 WHEN REPORT_planning_monthly_tb.audit_month BETWEEN 4 AND 6 THEN CONCAT("Month ", CAST(REPORT_planning_monthly_tb.audit_month - 3 as String))
   			 WHEN REPORT_planning_monthly_tb.audit_month BETWEEN 7 AND 9 THEN CONCAT("Month ", CAST(REPORT_planning_monthly_tb.audit_month - 6 AS string))
			 WHEN REPORT_planning_monthly_tb.audit_month BETWEEN 10 AND 12 THEN CONCAT("Month ", CAST(REPORT_planning_monthly_tb.audit_month - 9 AS string))
         END as audit_quarter_month
  			
  
  FROM REPORT_planning_monthly as REPORT_planning_monthly_tb
  LEFT JOIN reps_bcr as reps_bcr_tb
  ON REPORT_planning_monthly_tb.audit_year = reps_bcr_tb.audit_year
  AND REPORT_planning_monthly_tb.rep_id = reps_bcr_tb.rep_id

