/* NOT USED DUE TO COMPLEXITY */



WITH REPORT_planning_monthly AS

    (SELECT *
     FROM `star-big-data.star_us_rds.REPORT_planning_monthly` AS REPORT_planning_monthly_tb),
   
     yearScope AS
   
     (SELECT min(REPORT_planning_monthly_tb.audit_year) as audit_year_min,
             max(REPORT_planning_monthly_tb.audit_year) as audit_year_max
             
      FROM REPORT_planning_monthly as REPORT_planning_monthly_tb
     ),

     REPORT_planning_monthly_dates AS

     (SELECT REPORT_planning_monthly_tb.audit_year as audit_year,
              REPORT_planning_monthly_tb.audit_month as audit_month,
              REPORT_planning_monthly_tb.audit_quarter as audit_quarter,
              REPORT_planning_monthly_tb.rep_id as rep_id,
              REPORT_planning_monthly_tb.repName as repName,
              REPORT_planning_monthly_tb.is_before_previous_audit_year as is_before_previous_audit_year,
              REPORT_planning_monthly_tb.is_previous_audit_year as is_previous_audit_year,
              REPORT_planning_monthly_tb.is_current_audit_year as is_current_audit_year,
              REPORT_planning_monthly_tb.is_next_audit_year as is_next_audit_year


       FROM REPORT_planning_monthly AS REPORT_planning_monthly_tb

       GROUP BY audit_year,
                audit_quarter,
                audit_month,
                rep_id,
                repName,
                is_before_previous_audit_year,
                is_previous_audit_year,
                is_current_audit_year,
                is_next_audit_year),
   
    reps_bcr AS
    
      (SELECT reps_bcr_tb.year as audit_year,
              reps_bcr_tb.rep_id as rep_id,
              AVG((reps_bcr_tb.january_value
                  + reps_bcr_tb.february_value
                  + reps_bcr_tb.march_value
                  + reps_bcr_tb.april_value
                  + reps_bcr_tb.may_value
                  + reps_bcr_tb.june_value
                  + reps_bcr_tb.july_value
                  + reps_bcr_tb.august_value
                  + reps_bcr_tb.september_value
                  + reps_bcr_tb.october_value
                  + reps_bcr_tb.november_value
                  + reps_bcr_tb.december_value) / 12
              ) as value
              
       FROM `star-big-data.rep_dats_sheet.reps_bcr` as reps_bcr_tb
      
       WHERE reps_bcr_tb.year BETWEEN (SELECT audit_year_min FROM yearScope)
                                      AND (SELECT audit_year_max FROM yearScope)

       GROUP BY audit_year,
                rep_id

       )



SELECT "Header" as section,
       REPORT_planning_monthly_tb.rep_id as rep_id,
       REPORT_planning_monthly_tb.repName as repName,

       NULL as audit_quarter,

       NULL as audit_month,

       CONCAT("Compensation Plan For ", REPORT_planning_monthly_tb.repName, " Year ", CAST(current_audit_year + 1 as string), " (Next Year)") as dimension,

       -2 as dimension_order,

       NULL as metric_0,
       NULL as metric_1,
       NULL as metric_2,
       NULL as metric_3,

       NULL as metric_4,
       NULL as metric_5,
       NULL as metric_6

FROM REPORT_planning_monthly AS REPORT_planning_monthly_tb

GROUP BY section, rep_id, repName, dimension
/* 
UNION ALL

SELECT 'TopSummary' as section,
       REPORT_planning_monthly_tb.rep_id as rep_id,
       REPORT_planning_monthly_tb.repName as repName,

       CONCAT("Rep: ", REPORT_planning_monthly_tb.repName) as dimension,

       NULL as audit_quarter,

       NULL as audit_month,

       -1 as dimension_order,

       NULL as metric_0,
       NULL as metric_1,
       NULL as metric_2,
       NULL as metric_3,

       NULL as metric_4,
       NULL as metric_5,
       NULL as metric_6

FROM REPORT_planning_monthly AS REPORT_planning_monthly_tb

GROUP BY section,
         rep_id,
         repName
*/
UNION ALL
                

SELECT 'TopSummary' as section,
       REPORT_planning_monthly_tb.rep_id as rep_id,
       REPORT_planning_monthly_tb.repName as repName,

       NULL as audit_quarter,

       NULL as audit_month,

       CONCAT("Gross Margin in USD:") as dimension,

       0 as dimension_order,

       SUM(CASE WHEN REPORT_planning_monthly_tb.is_next_audit_year = True
                  AND REPORT_planning_monthly_tb.forecast_type_id = 1
                  AND REPORT_planning_monthly_tb.data_type = 'forecast'
                 THEN REPORT_planning_monthly_tb.plan_quota
                 ELSE 0
                 END) as metric_0,

       NULL as metric_1,
       NULL as metric_2,
       NULL as metric_3,

       NULL as metric_4,
       NULL as metric_5,
       NULL as metric_6

FROM REPORT_planning_monthly AS REPORT_planning_monthly_tb

GROUP BY section,
         rep_id,
         repName

UNION ALL

SELECT "TopSummary" as section,
        REPORT_planning_monthly_tb.rep_id as rep_id,
        REPORT_planning_monthly_tb.repName as repName,

        NULL as audit_quarter,

        NULL as audit_month,

        'BCR %:' as dimension,
        1 as dimension_order,
        round(AVG(reps_bcr_tb.value) * 100, 0) as metric_0,
        NULL as metric_1,
        NULL as metric_2,
        NULL as metric_3,

        NULL as metric_4,
        NULL as metric_5,
        NULL as metric_6

FROM REPORT_planning_monthly AS REPORT_planning_monthly_tb

INNER JOIN reps_bcr as reps_bcr_tb

ON REPORT_planning_monthly_tb.audit_year = reps_bcr_tb.audit_year
    AND REPORT_planning_monthly_tb.rep_id = reps_bcr_tb.rep_id

WHERE REPORT_planning_monthly_tb.is_next_audit_year = True

GROUP BY section,
         rep_id,
         repName


UNION ALL

SELECT "CompSummary" as section,
        year_rep_dim_tb.rep_id as rep_id,
        year_rep_dim_tb.repName as repName,

        NULL as audit_quarter,

        NULL as audit_month,


        "Gross Margin" as dimension,
        0 as dimension_order,
        SUM( CASE WHEN year_rep_dim_tb.is_before_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_0,

       SUM( CASE WHEN year_rep_dim_tb.is_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_1,

       SUM( CASE WHEN year_rep_dim_tb.is_current_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_2,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_3,



       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) * 0.75 as metric_4,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) * 1.2 as metric_5,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) * 1.5 as metric_6


FROM REPORT_planning_monthly_dates as year_rep_dim_tb


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_actuals_tb

ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_actuals_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_actuals_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_actuals_tb.rep_id
   AND REPORT_planning_monthly_actuals_tb.data_type = 'actual'


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_forecast_tb
ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_forecast_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_forecast_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_forecast_tb.rep_id
   AND REPORT_planning_monthly_forecast_tb.data_type = 'forecast'
   AND REPORT_planning_monthly_forecast_tb.forecast_type_id = 1


GROUP BY rep_id,
         repName

         
UNION ALL


SELECT "CompSummary" as section,
        year_rep_dim_tb.rep_id as rep_id,
        year_rep_dim_tb.repName as repName,

        NULL as audit_quarter,

        NULL as audit_month,

        "Base Pay" as dimension,
        1 as dimension_order,
        SUM( CASE WHEN year_rep_dim_tb.is_before_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_0,

       SUM( CASE WHEN year_rep_dim_tb.is_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_1,

       SUM( CASE WHEN year_rep_dim_tb.is_current_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_2,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_3,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_4,
       
       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_5,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_6


FROM REPORT_planning_monthly_dates as year_rep_dim_tb


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_actuals_tb

ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_actuals_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_actuals_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_actuals_tb.rep_id
   AND REPORT_planning_monthly_actuals_tb.data_type = 'actual'


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_forecast_tb
ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_forecast_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_forecast_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_forecast_tb.rep_id
   AND REPORT_planning_monthly_forecast_tb.data_type = 'forecast'
   AND REPORT_planning_monthly_forecast_tb.forecast_type_id = 1


GROUP BY rep_id,
         repName




UNION ALL


SELECT "CompSummary" as section,
        year_rep_dim_tb.rep_id as rep_id,
        year_rep_dim_tb.repName as repName,

        NULL as audit_quarter,

        NULL as audit_month,

        "Commission / Bonus" as dimension,
        2 as dimension_order,
        SUM( CASE WHEN year_rep_dim_tb.is_before_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_0,

       SUM( CASE WHEN year_rep_dim_tb.is_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_1,

       SUM( CASE WHEN year_rep_dim_tb.is_current_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_2,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_3,

       NULL as metric_4,

       NULL as metric_5,

       NULL as metric_6


FROM REPORT_planning_monthly_dates as year_rep_dim_tb


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_actuals_tb

ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_actuals_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_actuals_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_actuals_tb.rep_id
   AND REPORT_planning_monthly_actuals_tb.data_type = 'actual'


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_forecast_tb
ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_forecast_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_forecast_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_forecast_tb.rep_id
   AND REPORT_planning_monthly_forecast_tb.data_type = 'forecast'
   AND REPORT_planning_monthly_forecast_tb.forecast_type_id = 1


GROUP BY rep_id,
         repName




UNION ALL


SELECT "CompSummary" as section,
        year_rep_dim_tb.rep_id as rep_id,
        year_rep_dim_tb.repName as repName,

        NULL as audit_quarter,
        NULL as audit_month,
        "Total Earnings" as dimension,


        3 as dimension_order,
        SUM( CASE WHEN year_rep_dim_tb.is_before_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus + REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus + REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_0,

       SUM( CASE WHEN year_rep_dim_tb.is_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus + REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus + REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_1,

       SUM( CASE WHEN year_rep_dim_tb.is_current_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus + REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus + REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_2,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus + REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus + REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_3,

       NULL as metric_4,

       NULL as metric_5,

       NULL as metric_6


FROM REPORT_planning_monthly_dates as year_rep_dim_tb


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_actuals_tb

ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_actuals_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_actuals_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_actuals_tb.rep_id
   AND REPORT_planning_monthly_actuals_tb.data_type = 'actual'


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_forecast_tb
ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_forecast_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_forecast_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_forecast_tb.rep_id
   AND REPORT_planning_monthly_forecast_tb.data_type = 'forecast'
   AND REPORT_planning_monthly_forecast_tb.forecast_type_id = 1


GROUP BY rep_id,
         repName



UNION ALL


SELECT "CompSummary" as section,
        year_rep_dim_tb.rep_id as rep_id,
        year_rep_dim_tb.repName as repName,
        NULL as audit_quarter,
        NULL as audit_month,
        "Earnings as % of GM" as dimension,



        4 as dimension_order,
        SAFE_DIVIDE(SUM( CASE WHEN year_rep_dim_tb.is_before_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus + REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus + REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ),
           SUM( CASE WHEN year_rep_dim_tb.is_before_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           )) * 100
           
           AS metric_0,

       SAFE_DIVIDE(SUM( CASE WHEN year_rep_dim_tb.is_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus + REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus + REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           )
           , SUM( CASE WHEN year_rep_dim_tb.is_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           )) * 100
           
           AS metric_1,

       SAFE_DIVIDE(SUM( CASE WHEN year_rep_dim_tb.is_current_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus + REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus + REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           ) 
           , SUM( CASE WHEN year_rep_dim_tb.is_current_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           )) * 100
           
           AS metric_2,

       SAFE_DIVIDE(SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.commission + REPORT_planning_monthly_actuals_tb.qtly_bonus + REPORT_planning_monthly_actuals_tb.base_pay
                            ELSE REPORT_planning_monthly_forecast_tb.commission + REPORT_planning_monthly_forecast_tb.qtly_bonus + REPORT_planning_monthly_forecast_tb.base_pay
                       END
                  ELSE 0
                  END
             
             
           )
           , SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           )) * 100
           AS metric_3,

       
       NULL as metric_4,

       NULL as metric_5,

       NULL as metric_6


FROM REPORT_planning_monthly_dates as year_rep_dim_tb


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_actuals_tb

ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_actuals_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_actuals_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_actuals_tb.rep_id
   AND REPORT_planning_monthly_actuals_tb.data_type = 'actual'


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_forecast_tb
ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_forecast_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_forecast_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_forecast_tb.rep_id
   AND REPORT_planning_monthly_forecast_tb.data_type = 'forecast'
   AND REPORT_planning_monthly_forecast_tb.forecast_type_id = 1


GROUP BY rep_id,
         repName



UNION ALL


SELECT year_rep_dim_agg_year_tb.section as section,
       year_rep_dim_agg_year_tb.rep_id as rep_id,
       year_rep_dim_agg_year_tb.repName as repName,
       NULL as audit_quarter,
       NULL as audit_month,
       year_rep_dim_agg_year_tb.dimension as dimension,


       year_rep_dim_agg_year_tb.dimension_order as dimension_order,

       AVG( CASE WHEN year_rep_dim_agg_year_tb.is_before_previous_audit_year = True
                 THEN reps_bcr_tb.value * 100
                 ELSE NULL 
                 END
           ) AS metric_0,

       AVG( CASE WHEN year_rep_dim_agg_year_tb.is_previous_audit_year = True
                 THEN reps_bcr_tb.value * 100
                 ELSE NULL 
                 END
           ) AS metric_1,

       AVG( CASE WHEN year_rep_dim_agg_year_tb.is_current_audit_year = True
                 THEN reps_bcr_tb.value * 100
                 ELSE NULL 
                 END
           ) AS metric_2,

       AVG( CASE WHEN year_rep_dim_agg_year_tb.is_next_audit_year = True
                 THEN reps_bcr_tb.value * 100
                 ELSE NULL 
                 END
           ) AS metric_3,

       NULL as metric_4,

       NULL as metric_5,

       NULL as metric_6





FROM
       (SELECT "CompSummary" as section,
              year_rep_dim_tb.audit_year as audit_year,
              is_before_previous_audit_year,
              is_previous_audit_year,
              is_current_audit_year,
              is_next_audit_year,
              year_rep_dim_tb.rep_id as rep_id,
              year_rep_dim_tb.repName as repName,
              "Commission Rate %" as dimension,
              5 as dimension_order
              

       FROM REPORT_planning_monthly_dates as year_rep_dim_tb

       WHERE year_rep_dim_tb.is_before_previous_audit_year = True
       OR year_rep_dim_tb.is_previous_audit_year = True
       OR year_rep_dim_tb.is_current_audit_year = True
       OR year_rep_dim_tb.is_next_audit_year = True
       
       GROUP BY section,
              audit_year,
              is_before_previous_audit_year,
              is_previous_audit_year,
              is_current_audit_year,
              is_next_audit_year,
              rep_id,
              repName,
              dimension,
              dimension_order

       ) as year_rep_dim_agg_year_tb


LEFT JOIN reps_bcr AS reps_bcr_tb

ON year_rep_dim_agg_year_tb.audit_year = reps_bcr_tb.audit_year
       AND year_rep_dim_agg_year_tb.rep_id = reps_bcr_tb.rep_id




GROUP BY section,
         rep_id,
         repName,
         dimension,
         dimension_order



/* Quarterly Quota */
UNION ALL

SELECT "QuotaSummaryByQuarter" as section,
        year_rep_dim_tb.rep_id as rep_id,
        year_rep_dim_tb.repName as repName,

        year_rep_dim_tb.audit_quarter as audit_quarter,

        NULL as audit_month,
        
        "Gross Margin" as dimension,

        6 as dimension_order,
        SUM( CASE WHEN year_rep_dim_tb.is_before_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_0,

       SUM( CASE WHEN year_rep_dim_tb.is_previous_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_1,

       SUM( CASE WHEN year_rep_dim_tb.is_current_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_2,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_3,



       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) * 0.75 as metric_4,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) * 1.2 as metric_5,

       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) * 1.5 as metric_6


FROM REPORT_planning_monthly_dates as year_rep_dim_tb


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_actuals_tb

ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_actuals_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_actuals_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_actuals_tb.rep_id
   AND REPORT_planning_monthly_actuals_tb.data_type = 'actual'


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_forecast_tb
   ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_forecast_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_forecast_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_forecast_tb.rep_id
   AND REPORT_planning_monthly_forecast_tb.data_type = 'forecast'
   AND REPORT_planning_monthly_forecast_tb.forecast_type_id = 1


GROUP BY rep_id,
         repName,
         audit_quarter,
         audit_month




UNION ALL



SELECT "QuotaSummaryByQuarterMonth" as section,
        year_rep_dim_tb.rep_id as rep_id,
        year_rep_dim_tb.repName as repName,

        year_rep_dim_tb.audit_quarter as audit_quarter,
        
        year_rep_dim_tb.audit_month as audit_month,

        
        "Gross Margin" as dimension,

        7 as dimension_order,


       SUM( CASE WHEN year_rep_dim_tb.is_next_audit_year = True
                  THEN CASE WHEN TIMESTAMP_TRUNC(current_timestamp(), MONTH) > PARSE_TIMESTAMP("%Y-%m", CONCAT(CAST(year_rep_dim_tb.audit_year AS String), "-", CAST(year_rep_dim_tb.audit_month AS String)))
                            THEN REPORT_planning_monthly_actuals_tb.GM
                            ELSE REPORT_planning_monthly_forecast_tb.GM
                       END
                  ELSE 0
                  END
             
             
           ) AS metric_0,

       NULL as metric_1,

       NULL as metric_2,

       NULL as metric_3,

       NULL as metric_4,

       NULL as metric_5,

       NULL as metric_6




FROM REPORT_planning_monthly_dates as year_rep_dim_tb


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_actuals_tb

ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_actuals_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_actuals_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_actuals_tb.rep_id
   AND REPORT_planning_monthly_actuals_tb.data_type = 'actual'


LEFT JOIN REPORT_planning_monthly AS REPORT_planning_monthly_forecast_tb
   ON year_rep_dim_tb.audit_year = REPORT_planning_monthly_forecast_tb.audit_year
   AND year_rep_dim_tb.audit_month = REPORT_planning_monthly_forecast_tb.audit_month
   AND year_rep_dim_tb.rep_id = REPORT_planning_monthly_forecast_tb.rep_id
   AND REPORT_planning_monthly_forecast_tb.data_type = 'forecast'
   AND REPORT_planning_monthly_forecast_tb.forecast_type_id = 1


GROUP BY rep_id,
         repName,
         audit_quarter,
         audit_month