WITH dateSelector AS

       (SELECT CASE WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.start_date_ts) = 1
                    THEN TIMESTAMP_SUB(parsed_timestamps.start_date_ts, INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.start_date_ts) = 2
                    THEN TIMESTAMP_SUB(parsed_timestamps.start_date_ts, INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.start_date_ts) = 3
                    THEN TIMESTAMP_SUB(parsed_timestamps.start_date_ts, INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.start_date_ts) = 4
                    THEN TIMESTAMP_SUB(parsed_timestamps.start_date_ts, INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.start_date_ts) = 5
                    THEN TIMESTAMP_SUB(parsed_timestamps.start_date_ts, INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.start_date_ts) = 6
                    THEN TIMESTAMP_SUB(parsed_timestamps.start_date_ts, INTERVAL 6 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.start_date_ts) = 7
                    THEN parsed_timestamps.start_date_ts
               ELSE NULL
               END as start_date_ts,


          CASE WHEN @use_year_month = FALSE
               THEN CASE WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 1
                    THEN TIMESTAMP_ADD(parsed_timestamps.end_date_ts , INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 2
                    THEN TIMESTAMP_ADD(parsed_timestamps.end_date_ts , INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 3
                    THEN TIMESTAMP_ADD(parsed_timestamps.end_date_ts, INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 4
                    THEN TIMESTAMP_ADD(parsed_timestamps.end_date_ts, INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 5
                    THEN TIMESTAMP_ADD(parsed_timestamps.end_date_ts, INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 6
                    THEN parsed_timestamps.end_date_ts

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 7
                    THEN TIMESTAMP_ADD(parsed_timestamps.end_date_ts, INTERVAL 6 DAY)

               ELSE NULL END

               ELSE CASE WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts) = 1
                    THEN TIMESTAMP_SUB(parsed_timestamps.end_date_ts , INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 2
                    THEN TIMESTAMP_SUB(parsed_timestamps.end_date_ts , INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 3
                    THEN TIMESTAMP_SUB(parsed_timestamps.end_date_ts, INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 4
                    THEN TIMESTAMP_SUB(parsed_timestamps.end_date_ts, INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 5
                    THEN TIMESTAMP_SUB(parsed_timestamps.end_date_ts, INTERVAL 6 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 6
                    THEN parsed_timestamps.end_date_ts

                    WHEN EXTRACT(DAYOFWEEK FROM parsed_timestamps.end_date_ts ) = 7
                    THEN TIMESTAMP_SUB(parsed_timestamps.end_date_ts, INTERVAL 1 DAY)

               ELSE NULL END

               END as end_date_ts


          FROM

          (SELECT   CASE WHEN @use_year_month = FALSE
          
                         THEN
          
                              CASE WHEN PARSE_TIMESTAMP("%Y%m%d", @DS_END_DATE) > current_timestamp()
                                   THEN current_timestamp()
                                   ELSE PARSE_TIMESTAMP("%Y%m%d", @DS_END_DATE)
                              
                              END 

                         ELSE

                              CASE WHEN PARSE_TIMESTAMP("%Y%m%d", CONCAT(@year, @month, "01")) > current_timestamp()
                                   THEN CAST(LAST_DAY(current_date(), Month) as Timestamp)
                                   ELSE CAST(LAST_DAY(PARSE_DATE("%Y%m%d", CONCAT(@year, @month, "01"))) as Timestamp)
                              
                              END 

                         END AS end_date_ts,

                    

                    CASE WHEN @use_year_month = FALSE

                         THEN

                              CASE WHEN PARSE_TIMESTAMP("%Y%m%d", @DS_START_DATE) > current_timestamp()
                                   THEN current_timestamp()
                                   ELSE PARSE_TIMESTAMP("%Y%m%d", @DS_START_DATE)
                              END

                         ELSE

                              CASE WHEN PARSE_TIMESTAMP("%Y%m%d", CONCAT(@year, @month, "01")) > current_timestamp()
                                   THEN TIMESTAMP_TRUNC(current_timestamp(), Month)
                                   ELSE TIMESTAMP_TRUNC(PARSE_TIMESTAMP("%Y%m%d", CONCAT(@year, @month, "01")), Month)
                              
                              END 


                              
                    END as start_date_ts) as parsed_timestamps

               
          )

SELECT * FROM  `dummy.dummy.REPORT_spendLogAudit` as REPORT_spendLogAudit_tb
WHERE REPORT_spendLogAudit_tb.week_start_date >= (SELECT start_date_ts FROM dateSelector)
     AND REPORT_spendLogAudit_tb.week_end_date <= (SELECT end_date_ts FROM dateSelector)