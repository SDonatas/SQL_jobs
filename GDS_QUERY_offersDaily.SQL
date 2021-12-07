CREATE OR REPLACE PROCEDURE `dummy.dummy.ETL_REPORT_offers_daily`()
BEGIN

CREATE TEMP TABLE TEMP_currentWeekDaysEnhanced AS

WITH currentWeekDays AS

    (SELECT CAST(weekday_date as timestamp) as weekday_date

         FROM UNNEST(
             GENERATE_DATE_ARRAY(
                 CASE WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 1
                    THEN DATE_SUB(current_date(), INTERVAL 8 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 2
                    THEN DATE_SUB(current_date(), INTERVAL 9 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 3
                    THEN DATE_SUB(current_date(), INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 4
                    THEN DATE_SUB(current_date(), INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 5
                    THEN DATE_SUB(current_date(), INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 6
                    THEN DATE_SUB(current_date(), INTERVAL 6 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 7
                    THEN DATE_SUB(current_date(), INTERVAL 7 DAY)
               ELSE NULL END,


                    CASE WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 1
                    THEN DATE_SUB(current_date() , INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 2
                    THEN DATE_SUB(current_date() , INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 3
                    THEN DATE_ADD(current_date(), INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 4
                    THEN DATE_ADD(current_date(), INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 5
                    THEN DATE_ADD(current_date(), INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 6
                    THEN current_date()

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 7
                    THEN DATE_SUB(current_date(), INTERVAL 1 DAY)

               ELSE NULL END

             ) 

         ) as weekday_date

    )

    SELECT FORMAT_DATETIME("%A, %B %d, %Y", current_date()) as report_date,
            
            TIMESTAMP_TRUNC(
            CASE WHEN EXTRACT(DAYOFWEEK FROM current_timestamp()) = 1
                    THEN TIMESTAMP_SUB(current_timestamp(), INTERVAL 2 DAY)
                 WHEN EXTRACT(DAYOFWEEK FROM current_timestamp()) = 2
                    THEN TIMESTAMP_SUB(current_timestamp(), INTERVAL 3 DAY)
                 ELSE TIMESTAMP_SUB(current_timestamp(), INTERVAL 1 DAY)
            END, DAY) as previous_day_date_ts,

            currentWeekDays_tb.weekday_date as weekday_date_start_ts,
            timestamp_add(currentWeekDays_tb.weekday_date, INTERVAL 1 DAY) as weekday_date_end_ts,
            
            CASE WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 2 THEN "Mon"
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 3 THEN "Tue"
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 4 THEN "Wed"
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 5 THEN "Thu"
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 6 THEN "Fri"
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 7 THEN 'Sat'
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 1 THEN 'Sun'
             ELSE NULL END as weekday,
             
             /* Week start and Week end for joining plan data */
             CASE WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 1
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 2
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 3
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 4
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 5
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 6
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 6 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 7
                    THEN currentWeekDays_tb.weekday_date
               ELSE NULL
               END as week_date_start_ts,
               
               
               
               CASE WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 1
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date , INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 2
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date , INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 3
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date, INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 4
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date, INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 5
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date, INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 6
                    THEN currentWeekDays_tb.weekday_date

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 7
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date, INTERVAL 6 DAY)

               ELSE NULL END as week_date_end_ts
             
    
     FROM currentWeekDays as currentWeekDays_tb;



CREATE TEMP TABLE TEMP_currentQuarter AS

    SELECT min(dim_calendar_tb.date) as from_date,
            max(dim_calendar_tb.date) as to_date

    FROM `dummy.dummy.dim_calendar` as dim_calendar_tb

    WHERE dim_calendar_tb.audit_year IN (SELECT max(audit_year) FROM `dummy.dummy.dim_calendar` as dim_calendar_tb
                                          WHERE date IN (SELECT min(week_date_start_ts) FROM TEMP_currentWeekDaysEnhanced)
                                        )

    AND dim_calendar_tb.audit_quarter IN (SELECT max(audit_quarter) FROM `dummy.dummy.dim_calendar` as dim_calendar_tb
                                          WHERE date IN (SELECT min(week_date_start_ts) FROM TEMP_currentWeekDaysEnhanced));



CREATE TEMP TABLE TEMP_RAW_offers_quarter AS

    SELECT offers_tb.*,
           CAST(CASE WHEN reps_tb.timezoneid IS NULL OR reps_tb.timezoneid = 'Unassigned'
                THEN EXTRACT(DATETIME FROM offers_tb.offer_date)
                ELSE EXTRACT(DATETIME FROM offers_tb.offer_date AT TIME ZONE reps_tb.timezoneid)
                END AS TIMESTAMP) AS offer_date_dt_local_tz
    
    FROM `dummy.dummy.offers` as offers_tb

    LEFT JOIN `dummy.dummy.reps` as reps_tb

    ON offers_tb.rep_id = reps_tb.id

    WHERE offers_tb.offer_date >= (SELECT TIMESTAMP_SUB(from_date, INTERVAL 1 DAY) FROM TEMP_currentQuarter)
                     
                     AND

                     offers_tb.offer_date < (SELECT TIMESTAMP_ADD(to_date, INTERVAL 2 DAY) as to_date FROM TEMP_currentQuarter)
                                  
                      AND offers_tb.status != 'NoVisit';




CREATE TEMP TABLE TEMP_offersDaily_main AS

/*
WITH currentWeekDays AS

    (SELECT CAST(weekday_date as timestamp) as weekday_date

         FROM UNNEST(
             GENERATE_DATE_ARRAY(
                 CASE WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 1
                    THEN DATE_SUB(current_date(), INTERVAL 8 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 2
                    THEN DATE_SUB(current_date(), INTERVAL 9 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 3
                    THEN DATE_SUB(current_date(), INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 4
                    THEN DATE_SUB(current_date(), INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 5
                    THEN DATE_SUB(current_date(), INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 6
                    THEN DATE_SUB(current_date(), INTERVAL 6 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date()) = 7
                    THEN DATE_SUB(current_date(), INTERVAL 7 DAY)
               ELSE NULL END,


                    CASE WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 1
                    THEN DATE_SUB(current_date() , INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 2
                    THEN DATE_SUB(current_date() , INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 3
                    THEN DATE_ADD(current_date(), INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 4
                    THEN DATE_ADD(current_date(), INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 5
                    THEN DATE_ADD(current_date(), INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 6
                    THEN current_date()

                    WHEN EXTRACT(DAYOFWEEK FROM current_date() ) = 7
                    THEN DATE_SUB(current_date(), INTERVAL 1 DAY)

               ELSE NULL END

             ) 

         ) as weekday_date

    ),


  currentWeekDaysEnhanced AS

    (SELECT FORMAT_DATETIME("%A, %B %d, %Y", current_date()) as report_date,
            
            TIMESTAMP_TRUNC(
            CASE WHEN EXTRACT(DAYOFWEEK FROM current_timestamp()) = 1
                    THEN TIMESTAMP_SUB(current_timestamp(), INTERVAL 2 DAY)
                 WHEN EXTRACT(DAYOFWEEK FROM current_timestamp()) = 2
                    THEN TIMESTAMP_SUB(current_timestamp(), INTERVAL 3 DAY)
                 ELSE TIMESTAMP_SUB(current_timestamp(), INTERVAL 1 DAY)
            END, DAY) as previous_day_date_ts,

            currentWeekDays_tb.weekday_date as weekday_date_start_ts,
            timestamp_add(currentWeekDays_tb.weekday_date, INTERVAL 1 DAY) as weekday_date_end_ts,
            
            CASE WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 2 THEN "Mon"
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 3 THEN "Tue"
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 4 THEN "Wed"
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 5 THEN "Thu"
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 6 THEN "Fri"
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 7 THEN 'Sat'
                 WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 1 THEN 'Sun'
             ELSE NULL END as weekday,
             
             / Week start and Week end for joining plan data /
             CASE WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 1
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 2
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 3
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 4
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 5
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 6
                    THEN TIMESTAMP_SUB(currentWeekDays_tb.weekday_date, INTERVAL 6 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date) = 7
                    THEN currentWeekDays_tb.weekday_date
               ELSE NULL
               END as week_date_start_ts,
               
               
               
               CASE WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 1
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date , INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 2
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date , INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 3
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date, INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 4
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date, INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 5
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date, INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 6
                    THEN currentWeekDays_tb.weekday_date

                    WHEN EXTRACT(DAYOFWEEK FROM currentWeekDays_tb.weekday_date ) = 7
                    THEN TIMESTAMP_ADD(currentWeekDays_tb.weekday_date, INTERVAL 6 DAY)

               ELSE NULL END as week_date_end_ts
             
    
     FROM currentWeekDays as currentWeekDays_tb),

*/

WITH currentMonth AS

        (SELECT min(dim_calendar_tb.date) as from_date,
                max(dim_calendar_tb.date) as to_date

        FROM `dummy.dummy.dim_calendar` as dim_calendar_tb

        WHERE dim_calendar_tb.audit_year IN (SELECT max(audit_year) FROM `dummy.dummy.dim_calendar` as dim_calendar_tb
                                             WHERE date IN (SELECT min(week_date_start_ts) FROM TEMP_currentWeekDaysEnhanced)
                                            )

        AND dim_calendar_tb.audit_month IN (SELECT max(audit_month) FROM `dummy.dummy.dim_calendar` as dim_calendar_tb
                                             WHERE date IN (SELECT min(week_date_start_ts) FROM TEMP_currentWeekDaysEnhanced))
        
        ),

  /*
  currentQuarter AS


        (SELECT min(dim_calendar_tb.date) as from_date,
                max(dim_calendar_tb.date) as to_date

        FROM `dummy.dummy.dim_calendar` as dim_calendar_tb

        WHERE dim_calendar_tb.audit_year IN (SELECT max(audit_year) FROM `dummy.dummy.dim_calendar` as dim_calendar_tb
                                             WHERE date IN (SELECT min(week_date_start_ts) FROM currentWeekDaysEnhanced)
                                            )

        AND dim_calendar_tb.audit_quarter IN (SELECT max(audit_quarter) FROM `dummy.dummy.dim_calendar` as dim_calendar_tb
                                             WHERE date IN (SELECT min(week_date_start_ts) FROM currentWeekDaysEnhanced))
        
        ),

  */

  dailyReport AS
     
        (SELECT
          "Daily" as section,
          currentWeekDaysEnhanced_tb.report_date as report_date,

          CASE WHEN currentWeekDaysEnhanced_tb.previous_day_date_ts = currentWeekDaysEnhanced_tb.weekday_date_start_ts
              THEN True
              ELSE False
              END as is_previous_day,

          currentWeekDaysEnhanced_tb.weekday_date_start_ts as weekday_date_start_ts,
          currentWeekDaysEnhanced_tb.weekday_date_end_ts as weekday_date_end_ts,
          currentWeekDaysEnhanced_tb.week_date_start_ts as week_date_start_ts,
          currentWeekDaysEnhanced_tb.week_date_end_ts as week_date_end_ts,

          currentWeekDaysEnhanced_tb.weekday as weekday,

          reps_tb.name as reps_name,
          dim_teams_tb.team_name as reps_team_name,
          reps_team_tb.is_leader as reps_is_leader,


          SUM(CASE WHEN offers_tb.total_offer IS NULL OR offers_tb.status != 'Accepted' THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.total_offer AS BIGDECIMAL) END) as offers_total_offer,
          SUM(CASE WHEN offers_tb.id IS NOT NULL AND offers_tb.status != 'NoVisit'  THEN 1 ELSE 0 END) as offers_total_visits,
          SUM(CASE WHEN offers_tb.id IS NOT NULL AND offers_tb.status = 'Accepted' THEN 1 ELSE 0 END) as offers_total_buys,



SUM(CASE WHEN offers_tb.status = 'Accepted' THEN
          (
            (
                dummy.bigDecimal_division(CASE WHEN offers_tb.au_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.au_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_au_grams AS BIGDECIMAL) END 
                  + CASE WHEN offers_tb.yr_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_au_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_au_grams AS BIGDECIMAL) END
                  )
              ) +

            (
                dummy.bigDecimal_division(CASE WHEN offers_tb.ag_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.ag_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_ag_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yr_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_ag_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_ag_grams AS BIGDECIMAL)  END
                  )
            ) +
            (
                dummy.bigDecimal_division(CASE WHEN offers_tb.pd_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pd_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_pd_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yr_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_pd_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_pd_grams AS BIGDECIMAL) END)
            ) +
            (
              dummy.bigDecimal_division(CASE WHEN offers_tb.pt_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pt_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_pt_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yr_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_pt_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_pt_grams AS BIGDECIMAL) END
                  )
              )
          )

          + (
              (
                 dummy.bigDecimal_division(CASE WHEN offers_tb.au_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.au_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_au_grams AS BIGDECIMAL) END
                      + CASE WHEN offers_tb.wd_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_au_grams AS BIGDECIMAL) END
                      )
                ) +

              (
                 dummy.bigDecimal_division(CASE WHEN offers_tb.ag_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.ag_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_ag_grams AS BIGDECIMAL) END
                      + CASE WHEN offers_tb.wd_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_ag_grams AS BIGDECIMAL) END
                    )
              ) +
              (
                 dummy.bigDecimal_division(CASE WHEN offers_tb.pd_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pd_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_pd_grams AS BIGDECIMAL) END
                      + CASE WHEN offers_tb.wd_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_pd_grams AS BIGDECIMAL) END
                      )
              ) +
              (
                dummy.bigDecimal_division(CASE WHEN offers_tb.pt_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pt_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_pt_grams AS BIGDECIMAL) END
                    + CASE WHEN offers_tb.wd_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_pt_grams AS BIGDECIMAL) END))
          )
          ELSE CAST(0 AS BIGDECIMAL) END) as calc_est_mkt_val,

          /* allocation to 5 days */
          AVG(CAST(plan_quota_tb.plan_quota AS BIGDECIMAL)) / 5 as plan_quota,

          /* allocation to 7 days */
          AVG(CAST(plan_quota_tb.plan_quota AS BIGDECIMAL)) / 7 as plan_quota_full_week

      FROM TEMP_currentWeekDaysEnhanced as currentWeekDaysEnhanced_tb
      CROSS JOIN (SELECT id,
                         name
                  FROM `dummy.dummy.reps`
                  WHERE exclude_from_reports = False AND inactive = False
                       AND name not in ('General Pot')
                  
                  ) as reps_tb


      LEFT JOIN TEMP_RAW_offers_quarter as offers_tb
      ON reps_tb.id = offers_tb.rep_id
          AND currentWeekDaysEnhanced_tb.weekday_date_start_ts <= offers_tb.offer_date_dt_local_tz
          AND currentWeekDaysEnhanced_tb.weekday_date_end_ts > offers_tb.offer_date_dt_local_tz
          AND offers_tb.status != 'NoVisit'

      /* Join plan weekly data */
      LEFT JOIN  `dummy.dummy.transformed_reps_plan_quota` as plan_quota_tb
      ON reps_tb.id = plan_quota_tb.rep_id
          AND currentWeekDaysEnhanced_tb.week_date_start_ts = plan_quota_tb.week_start_ts 
            AND currentWeekDaysEnhanced_tb.week_date_end_ts = plan_quota_tb.week_end_ts

      /* Join team information */
      LEFT JOIN `dummy.rep_dats_sheet.reps_team` as reps_team_tb
      ON reps_tb.id = reps_team_tb.rep_id

      LEFT JOIN `dummy.rep_dats_sheet.dim_teams` as dim_teams_tb
      ON reps_team_tb.team_id = dim_teams_tb.team_id




      GROUP BY section,
                report_date,
                is_previous_day,
                weekday_date_start_ts,
                weekday_date_end_ts,
                week_date_start_ts,
                week_date_end_ts,
                weekday,
                reps_tb.id,
                reps_name,
                reps_team_name,
                reps_is_leader
                
                ),
          
  monthlyReport AS          
      
      (SELECT section,
             report_date,
             is_previous_day,
             weekday_date_start_ts,
             weekday_date_end_ts,
             week_date_start_ts,
             week_date_end_ts,
             weekday,
             reps_name,
             dim_teams_tb.team_name as reps_team_name,
             reps_team_tb.is_leader as reps_is_leader,
             offers_total_offer,
             offers_total_visits,
             offers_total_buys,
             calc_est_mkt_val,
             plan_quota_tb.plan_quota as plan_quota
             
      FROM
            
      (SELECT 'MTD' as section,
             FORMAT_DATETIME("%A, %B %d, %Y", current_date()) as report_date,
             False as is_previous_day,
             PARSE_TIMESTAMP("%Y%m%d", "19000101") as weekday_date_start_ts,
             PARSE_TIMESTAMP("%Y%m%d", "19000101") as weekday_date_end_ts,
             PARSE_TIMESTAMP("%Y%m%d", "19000101") as week_date_start_ts,
             PARSE_TIMESTAMP("%Y%m%d", "19000101") as week_date_end_ts,
             "N/A" as weekday,
             reps_tb.id as rep_id,
             reps_tb.name as reps_name,

             SUM(offers_tb.offers_total_offer) as offers_total_offer,
             SUM(offers_tb.offers_total_visits) as offers_total_visits,
             SUM(offers_tb.offers_total_buys) as offers_total_buys,
             SUM(offers_tb.calc_est_mkt_val) as calc_est_mkt_val


    FROM  (SELECT id,
                  name
           FROM `dummy.dummy.reps`
           WHERE exclude_from_reports = False
                AND name not in ('General Pot')
           ) as reps_tb

    LEFT JOIN (SELECT  rep_id,
                       SUM(CASE WHEN offers_tb.total_offer IS NULL OR offers_tb.status != 'Accepted' THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.total_offer AS BIGDECIMAL) END) as offers_total_offer,
                       SUM(CASE WHEN offers_tb.status != 'NoVisit' THEN 1 ELSE 0 END) as offers_total_visits,
                       SUM(CASE WHEN offers_tb.status = 'Accepted' THEN 1 ELSE 0 END) as offers_total_buys,


SUM(CASE WHEN offers_tb.status = 'Accepted' THEN
          (
            (
                dummy.bigDecimal_division(CASE WHEN offers_tb.au_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.au_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_au_grams AS BIGDECIMAL) END 
                  + CASE WHEN offers_tb.yr_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_au_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_au_grams AS BIGDECIMAL) END
                  )
              ) +

            (
                dummy.bigDecimal_division(CASE WHEN offers_tb.ag_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.ag_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_ag_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yr_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_ag_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_ag_grams AS BIGDECIMAL)  END
                  )
            ) +
            (
                dummy.bigDecimal_division(CASE WHEN offers_tb.pd_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pd_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_pd_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yr_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_pd_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_pd_grams AS BIGDECIMAL) END)
            ) +
            (
              dummy.bigDecimal_division(CASE WHEN offers_tb.pt_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pt_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_pt_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yr_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_pt_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_pt_grams AS BIGDECIMAL) END
                  )
              )
          )

          + (
              (
                 dummy.bigDecimal_division(CASE WHEN offers_tb.au_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.au_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_au_grams AS BIGDECIMAL) END
                      + CASE WHEN offers_tb.wd_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_au_grams AS BIGDECIMAL) END
                      )
                ) +

              (
                 dummy.bigDecimal_division(CASE WHEN offers_tb.ag_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.ag_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_ag_grams AS BIGDECIMAL) END
                      + CASE WHEN offers_tb.wd_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_ag_grams AS BIGDECIMAL) END
                    )
              ) +
              (
                 dummy.bigDecimal_division(CASE WHEN offers_tb.pd_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pd_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_pd_grams AS BIGDECIMAL) END
                      + CASE WHEN offers_tb.wd_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_pd_grams AS BIGDECIMAL) END
                      )
              ) +
              (
                dummy.bigDecimal_division(CASE WHEN offers_tb.pt_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pt_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_pt_grams AS BIGDECIMAL) END
                    + CASE WHEN offers_tb.wd_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_pt_grams AS BIGDECIMAL) END))
          )
          ELSE CAST(0 AS BIGDECIMAL) END) as calc_est_mkt_val
    
                
                FROM TEMP_RAW_offers_quarter as offers_tb
                
                WHERE offers_tb.offer_date_dt_local_tz >= (SELECT from_date FROM currentMonth)

                AND
           
                 offers_tb.offer_date_dt_local_tz < (SELECT TIMESTAMP_ADD(to_date, INTERVAL 1 DAY) as to_date FROM currentMonth)
    
                AND offers_tb.status != 'NoVisit' AND offers_tb.id IS NOT NULL
                      
             
              GROUP BY rep_id
             
             
             
             
             )as offers_tb
    
    
    
    
    
    ON reps_tb.id = offers_tb.rep_id

    


    GROUP BY section,
             report_date,
             is_previous_day,
             weekday_date_start_ts,
             weekday_date_end_ts,
             week_date_start_ts,
             week_date_end_ts,
             weekday,
             rep_id,
             reps_name

           ) monthly_tb
           
           
           LEFT JOIN (SELECT plan_quota_tb.rep_id, sum(plan_quota_tb.plan_quota) as plan_quota FROM `dummy.dummy.transformed_reps_plan_quota`  as plan_quota_tb

               WHERE plan_quota_tb.week_start_ts >= (SELECT from_date FROM currentMonth)


                AND plan_quota_tb.week_end_ts <= (SELECT to_date FROM currentMonth)
                GROUP BY rep_id


                ) as plan_quota_tb

            ON monthly_tb.rep_id = plan_quota_tb.rep_id

           /* Join team information */
           LEFT JOIN `dummy.rep_dats_sheet.reps_team` as reps_team_tb
           ON monthly_tb.rep_id = reps_team_tb.rep_id

           LEFT JOIN `dummy.rep_dats_sheet.dim_teams` as dim_teams_tb
           ON reps_team_tb.team_id = dim_teams_tb.team_id
            
     ),


quarterlyReport AS
      
      (SELECT section,
             report_date,
             is_previous_day,
             weekday_date_start_ts,
             weekday_date_end_ts,
             week_date_start_ts,
             week_date_end_ts,
             weekday,
             reps_name,
             dim_teams_tb.team_name as reps_team_name,
             reps_team_tb.is_leader as reps_is_leader,
             offers_total_offer,
             offers_total_visits,
             offers_total_buys,
             calc_est_mkt_val,
             plan_quota_tb.plan_quota as plan_quota
             
      FROM
            
      (SELECT 'QTD' as section,
             FORMAT_DATETIME("%A, %B %d, %Y", current_date()) as report_date,
             False as is_previous_day,
             PARSE_TIMESTAMP("%Y%m%d", "19000101") as weekday_date_start_ts,
             PARSE_TIMESTAMP("%Y%m%d", "19000101") as weekday_date_end_ts,
             PARSE_TIMESTAMP("%Y%m%d", "19000101") as week_date_start_ts,
             PARSE_TIMESTAMP("%Y%m%d", "19000101") as week_date_end_ts,
             "N/A" as weekday,
             reps_tb.id as rep_id,
             reps_tb.name as reps_name,
             SUM(offers_tb.offers_total_offer) as offers_total_offer,             
             SUM(offers_tb.offers_total_visits) as offers_total_visits,
             SUM(offers_tb.offers_total_buys) as offers_total_buys,
             SUM(offers_tb.calc_est_mkt_val) as calc_est_mkt_val

   
    FROM  (SELECT id,
                  name
           FROM `dummy.dummy.reps`
           WHERE exclude_from_reports = False
                AND name not in ('General Pot')
           ) as reps_tb

    LEFT JOIN (SELECT rep_id,
                      SUM(CASE WHEN offers_tb.total_offer IS NULL OR offers_tb.status != 'Accepted' THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.total_offer AS BIGDECIMAL) END) as offers_total_offer,
                      SUM(CASE WHEN offers_tb.id IS NULL THEN 0 ELSE 1 END) as offers_total_visits,
                      SUM(CASE WHEN offers_tb.id IS NOT NULL AND offers_tb.status = 'Accepted' THEN 1 ELSE 0 END) as offers_total_buys,
                      
SUM(CASE WHEN offers_tb.status = 'Accepted' THEN
          (
            (
                dummy.bigDecimal_division(CASE WHEN offers_tb.au_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.au_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_au_grams AS BIGDECIMAL) END 
                  + CASE WHEN offers_tb.yr_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_au_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_au_grams AS BIGDECIMAL) END
                  )
              ) +

            (
                dummy.bigDecimal_division(CASE WHEN offers_tb.ag_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.ag_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_ag_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yr_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_ag_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_ag_grams AS BIGDECIMAL)  END
                  )
            ) +
            (
                dummy.bigDecimal_division(CASE WHEN offers_tb.pd_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pd_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_pd_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yr_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_pd_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_pd_grams AS BIGDECIMAL) END)
            ) +
            (
              dummy.bigDecimal_division(CASE WHEN offers_tb.pt_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pt_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
              * (CASE WHEN offers_tb.yc_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yc_pt_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yr_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yr_pt_grams AS BIGDECIMAL) END
                  + CASE WHEN offers_tb.yd_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.yd_pt_grams AS BIGDECIMAL) END
                  )
              )
          )

          + (
              (
                 dummy.bigDecimal_division(CASE WHEN offers_tb.au_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.au_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_au_grams AS BIGDECIMAL) END
                      + CASE WHEN offers_tb.wd_au_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_au_grams AS BIGDECIMAL) END
                      )
                ) +

              (
                 dummy.bigDecimal_division(CASE WHEN offers_tb.ag_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.ag_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_ag_grams AS BIGDECIMAL) END
                      + CASE WHEN offers_tb.wd_ag_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_ag_grams AS BIGDECIMAL) END
                    )
              ) +
              (
                 dummy.bigDecimal_division(CASE WHEN offers_tb.pd_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pd_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_pd_grams AS BIGDECIMAL) END
                      + CASE WHEN offers_tb.wd_pd_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_pd_grams AS BIGDECIMAL) END
                      )
              ) +
              (
                dummy.bigDecimal_division(CASE WHEN offers_tb.pt_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pt_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL))
                  * (CASE WHEN offers_tb.wc_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wc_pt_grams AS BIGDECIMAL) END
                    + CASE WHEN offers_tb.wd_pt_grams IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.wd_pt_grams AS BIGDECIMAL) END))
          )
          ELSE CAST(0 AS BIGDECIMAL) END) as calc_est_mkt_val
                      
               
               
               
               FROM TEMP_RAW_offers_quarter as offers_tb
               
               
               WHERE offers_tb.offer_date_dt_local_tz >= (SELECT from_date FROM TEMP_currentQuarter)
                     
                     AND

                     offers_tb.offer_date_dt_local_tz < (SELECT TIMESTAMP_ADD(to_date, INTERVAL 1 DAY) as to_date FROM TEMP_currentQuarter)
                                  
                      AND offers_tb.status != 'NoVisit'
                      
               
               GROUP BY rep_id
               
               ) as offers_tb
               
               
    ON reps_tb.id = offers_tb.rep_id

   
                                  
                                  


    GROUP BY section,
             report_date,
             is_previous_day,
             weekday_date_start_ts,
             weekday_date_end_ts,
             week_date_start_ts,
             week_date_end_ts,
             weekday,
             rep_id,
             reps_name

           ) quarterly_tb
           
           
           LEFT JOIN (SELECT plan_quota_tb.rep_id, sum(plan_quota_tb.plan_quota) as plan_quota FROM `dummy.dummy.transformed_reps_plan_quota`  as plan_quota_tb

               WHERE plan_quota_tb.week_start_ts >= (SELECT from_date FROM TEMP_currentQuarter)


                AND plan_quota_tb.week_end_ts <= (SELECT to_date FROM TEMP_currentQuarter)

                GROUP BY rep_id


                ) as plan_quota_tb

            ON quarterly_tb.rep_id = plan_quota_tb.rep_id

            LEFT JOIN `dummy.rep_dats_sheet.reps_team` as reps_team_tb

            ON quarterly_tb.rep_id = reps_team_tb.rep_id

            LEFT JOIN `dummy.rep_dats_sheet.dim_teams` as dim_teams_tb

            ON reps_team_tb.team_id = dim_teams_tb.team_id
            
     )



        SELECT dailyReport_ranked_tb.*,
                RANK() OVER (PARTITION BY weekday_date_start_ts ORDER BY calc_target_gm_percent DESC) as calc_target_gm_percent_rank
        
        FROM
        (SELECT dailyReport_tb.section,
                dailyReport_tb.report_date,
                dailyReport_tb.is_previous_day,
                dailyReport_tb.weekday_date_start_ts,
                dailyReport_tb.weekday_date_end_ts,
                dailyReport_tb.week_date_start_ts,
                dailyReport_tb.week_date_end_ts,
                dailyReport_tb.weekday,
                dailyReport_tb.reps_name,
                dailyReport_tb.reps_team_name,
                dailyReport_tb.reps_is_leader,
                dailyReport_tb.offers_total_offer,
                dailyReport_tb.offers_total_visits,
                dailyReport_tb.offers_total_buys,
                dailyReport_tb.calc_est_mkt_val,
                dailyReport_tb.plan_quota,
        
                (dailyReport_tb.calc_est_mkt_val - dailyReport_tb.offers_total_offer) as calc_gross_margin,
                
                CASE WHEN dailyReport_tb.calc_est_mkt_val IS NULL OR dailyReport_tb.calc_est_mkt_val = 0 THEN 0
                ELSE round(dailyReport_tb.offers_total_offer / dailyReport_tb.calc_est_mkt_val, 2)
                END as calc_gross_margin_percent,
                
                CASE WHEN dailyReport_tb.plan_quota IS NULL OR dailyReport_tb.plan_quota = 0 THEN 0
                ELSE (dailyReport_tb.calc_est_mkt_val - dailyReport_tb.offers_total_offer) / dailyReport_tb.plan_quota
                END AS calc_target_gm_percent    
        
        FROM dailyReport as dailyReport_tb) as dailyReport_ranked_tb
        
        
        UNION ALL
        
        
        SELECT monthlyReport_ranked_tb.*,
                RANK() OVER (PARTITION BY weekday_date_start_ts ORDER BY calc_target_gm_percent DESC) as calc_target_gm_percent_rank
                
        FROM
        (SELECT monthlyReport_tb.*,
                (monthlyReport_tb.calc_est_mkt_val - monthlyReport_tb.offers_total_offer) as calc_gross_margin,
                
                CASE WHEN monthlyReport_tb.calc_est_mkt_val IS NULL OR monthlyReport_tb.calc_est_mkt_val = 0 THEN 0
                ELSE round(monthlyReport_tb.offers_total_offer / monthlyReport_tb.calc_est_mkt_val, 2)
                END as calc_gross_margin_percent,
                
                CASE WHEN monthlyReport_tb.plan_quota IS NULL OR monthlyReport_tb.plan_quota = 0 THEN 0
                ELSE (monthlyReport_tb.calc_est_mkt_val - monthlyReport_tb.offers_total_offer) / monthlyReport_tb.plan_quota
                END AS calc_target_gm_percent      
          
          FROM monthlyReport as monthlyReport_tb) AS monthlyReport_ranked_tb
        
        UNION ALL
          
          SELECT quarterlyReport_ranked_tb.*,
                RANK() OVER (PARTITION BY weekday_date_start_ts ORDER BY calc_target_gm_percent DESC) as calc_target_gm_percent_rank
                  
          
          FROM
          (SELECT quarterlyReport_tb.*,
                  (quarterlyReport_tb.calc_est_mkt_val - quarterlyReport_tb.offers_total_offer) as calc_gross_margin,
                
                  CASE WHEN quarterlyReport_tb.calc_est_mkt_val IS NULL OR quarterlyReport_tb.calc_est_mkt_val = 0 THEN 0
                  ELSE round(quarterlyReport_tb.offers_total_offer / quarterlyReport_tb.calc_est_mkt_val, 2)
                  END as calc_gross_margin_percent,

                  CASE WHEN quarterlyReport_tb.plan_quota IS NULL OR quarterlyReport_tb.plan_quota = 0 THEN 0
                  ELSE (quarterlyReport_tb.calc_est_mkt_val - quarterlyReport_tb.offers_total_offer) / quarterlyReport_tb.plan_quota
                  END AS calc_target_gm_percent      
          
          FROM quarterlyReport as quarterlyReport_tb) AS quarterlyReport_ranked_tb
          
          
        UNION ALL
        
        /* WTD Data */
        
        SELECT weeklyReport_ranked_tb.*,
                RANK() OVER (PARTITION BY weekday_date_start_ts ORDER BY calc_target_gm_percent DESC) as calc_target_gm_percent_rank
        
        FROM
        
        (SELECT weeklyReport_tb.*,
                (weeklyReport_tb.calc_est_mkt_val - weeklyReport_tb.offers_total_offer) as calc_gross_margin,
                
                CASE WHEN weeklyReport_tb.calc_est_mkt_val IS NULL OR weeklyReport_tb.calc_est_mkt_val = 0 THEN 0
                ELSE round(weeklyReport_tb.offers_total_offer / weeklyReport_tb.calc_est_mkt_val, 2)
                END as calc_gross_margin_percent,
                
                CASE WHEN weeklyReport_tb.plan_quota IS NULL OR weeklyReport_tb.plan_quota = 0 THEN 0
                ELSE (weeklyReport_tb.calc_est_mkt_val - weeklyReport_tb.offers_total_offer) / weeklyReport_tb.plan_quota
                END AS calc_target_gm_percent    
                
        
        FROM
        (SELECT 'WTD' as section,
                FORMAT_DATETIME("%A, %B %d, %Y", current_date()) as report_date,
                False as is_previous_day,
                PARSE_TIMESTAMP("%Y%m%d", "19000101") as weekday_date_start_ts,
                PARSE_TIMESTAMP("%Y%m%d", "19000101") as weekday_date_end_ts,
                PARSE_TIMESTAMP("%Y%m%d", "19000101") as week_date_start_ts,
                PARSE_TIMESTAMP("%Y%m%d", "19000101") as week_date_end_ts,
                "N/A" as weekday,
                reps_name,
                reps_team_name,
                reps_is_leader,
                SUM(offers_total_offer) as offers_total_offer,
                SUM(offers_total_visits) as offers_total_visits,
                SUM(offers_total_buys) as offers_total_buys,
                SUM(calc_est_mkt_val) as calc_est_mkt_val,
                SUM(plan_quota_full_week) as plan_quota,
                    
          FROM dailyReport
          
          GROUP BY section,
                  report_date,
                  is_previous_day,
                  weekday_date_start_ts,
                  weekday_date_end_ts,
                  week_date_start_ts,
                  week_date_end_ts,
                  weekday,
                  reps_name,
                  reps_team_name,
                  reps_is_leader
          
          ) as weeklyReport_tb
        
        
        ) as weeklyReport_ranked_tb;

CREATE TEMP TABLE LoadTable_REPORT_offers_daily AS 

  WITH topTeamsDaily AS

        (SELECT mainReport_tb.weekday_date_start_ts as weekday_date_start_ts,
                mainReport_tb.weekday_date_end_ts as weekday_date_end_ts,
                mainReport_tb.reps_team_name as reps_team_name,
                mainReport_tb.section as section,
                (SUM(mainReport_tb.calc_est_mkt_val) - SUM(mainReport_tb.offers_total_offer)) / SUM(mainReport_tb.plan_quota) as calc_team_target_percent

         FROM TEMP_offersDaily_main as mainReport_tb

         WHERE mainReport_tb.reps_team_name IS NOT NULL AND mainReport_tb.section = 'Daily'

         GROUP BY weekday_date_start_ts, weekday_date_end_ts, reps_team_name, section

        ),

  topTeamsDailyEnhanced AS

      (SELECT topTeamsDaily_tb.*,
              CASE WHEN topTeamsDaily_tb.calc_team_target_percent = topTeamsDaily_top_tb.calc_team_target_percent_max
                   THEN True
                   ELSE False
                   END is_top_team

       FROM topTeamsDaily as topTeamsDaily_tb

       LEFT JOIN (SELECT weekday_date_start_ts,
                         weekday_date_end_ts,
                         section,
                         max(calc_team_target_percent) as calc_team_target_percent_max

                  FROM topTeamsDaily
                  
                  GROUP BY weekday_date_start_ts,
                           weekday_date_end_ts,
                           section
                   
                  ) as topTeamsDaily_top_tb

      ON topTeamsDaily_tb.weekday_date_start_ts = topTeamsDaily_top_tb.weekday_date_start_ts
         AND topTeamsDaily_tb.weekday_date_end_ts = topTeamsDaily_top_tb.weekday_date_end_ts
         AND topTeamsDaily_tb.section = topTeamsDaily_top_tb.section
       
       ),

  topTeamsWeekly AS

      (SELECT mainReport_tb.reps_team_name as reps_team_name,
              mainReport_tb.section as section,
              (SUM(mainReport_tb.calc_est_mkt_val) - SUM(mainReport_tb.offers_total_offer)) / SUM(mainReport_tb.plan_quota) as calc_team_target_percent


       FROM TEMP_offersDaily_main as mainReport_tb

       WHERE mainReport_tb.reps_team_name IS NOT NULL AND mainReport_tb.section = 'WTD'
       
       GROUP BY reps_team_name, section

      ),


  topTeamWeeklyEnhanced AS

      (SELECT topTeamsWeekly_tb.*,
              CASE WHEN topTeamsWeekly_tb.calc_team_target_percent = topTeamsWeekly_top_tb.calc_team_target_percent_max
                   THEN True
                   ELSE False
                   END is_top_team
              

       FROM topTeamsWeekly as topTeamsWeekly_tb

       LEFT JOIN (SELECT topTeamsWeekly_top_tb.section as section,
                         MAX(topTeamsWeekly_top_tb.calc_team_target_percent) as calc_team_target_percent_max

                  FROM topTeamsWeekly as topTeamsWeekly_top_tb

                  GROUP BY section) as topTeamsWeekly_top_tb
      
       ON topTeamsWeekly_tb.section = topTeamsWeekly_top_tb.section

      )

        

SELECT mainReport_tb.*,
       CASE WHEN mainReport_tb.section = 'Daily'
            THEN topTeamsDailyEnhanced_tb.calc_team_target_percent

            WHEN mainReport_tb.section = 'WTD'
            THEN topTeamWeeklyEnhanced_tb.calc_team_target_percent

            ELSE NULL END as calc_team_target_percent,
      
       CASE WHEN mainReport_tb.section = 'Daily'
            THEN topTeamsDailyEnhanced_tb.is_top_team

            WHEN mainReport_tb.section = 'WTD'
            THEN topTeamWeeklyEnhanced_tb.is_top_team

            ELSE NULL END as is_top_daily_team
            

FROM TEMP_offersDaily_main as mainReport_tb

/* Daily Top Team */
LEFT JOIN topTeamsDailyEnhanced as topTeamsDailyEnhanced_tb

ON mainReport_tb.weekday_date_start_ts = topTeamsDailyEnhanced_tb.weekday_date_start_ts
    AND mainReport_tb.weekday_date_end_ts = topTeamsDailyEnhanced_tb.weekday_date_end_ts
    AND mainReport_tb.reps_team_name = topTeamsDailyEnhanced_tb.reps_team_name
    AND mainReport_tb.section = topTeamsDailyEnhanced_tb.section

/* Weekly Top Team */
LEFT JOIN topTeamWeeklyEnhanced as topTeamWeeklyEnhanced_tb
ON mainReport_tb.reps_team_name = topTeamWeeklyEnhanced_tb.reps_team_name
    AND mainReport_tb.section = topTeamWeeklyEnhanced_tb.section;



IF (SELECT count(*) FROM dummy.__TABLES__ WHERE table_id='REPORT_offers_daily') = 0 THEN
  CREATE TABLE IF NOT EXISTS `dummy.REPORT_offers_daily`
  AS
  SELECT * FROM LoadTable_REPORT_offers_daily;

ELSE
  BEGIN
  /* DELETE OLD DATA */
  TRUNCATE TABLE `dummy.REPORT_offers_daily`;

  /* Load new data into a table */
  INSERT INTO `dummy.REPORT_offers_daily`
  SELECT * FROM LoadTable_REPORT_offers_daily;
  END;


END IF;
END;