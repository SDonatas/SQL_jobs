CREATE OR REPLACE PROCEDURE `dummy.dummy.ETL_AUDIT_REPORT_offersConsolidated`(start_week_ts TIMESTAMP, end_week_ts TIMESTAMP)
BEGIN

/* Variables */
DECLARE current_timestamp_var TIMESTAMP;
SET current_timestamp_var = CURRENT_TIMESTAMP();


CREATE TEMP TABLE TEMP_AUDIT_REPORT_offersConsolidated AS

  WITH selectedSpendlogAuditIds AS

    (SELECT spendlog_audit_tb.id AS spendlog_audit_id
     FROM `dummy.dummy.spendlog_audit` as spendlog_audit_tb
     WHERE spendlog_audit_tb.week_start_date >= start_week_ts
          AND spendlog_audit_tb.week_end_date <= end_week_ts
    ),

  exceptionsRepId AS

    (SELECT 43 as id),

  selectedOfferIds AS

  (SELECT offers_tb.id as offer_id
    FROM `dummy.dummy.offers` as offers_tb
      WHERE offers_tb.spendlog_audit_id IN (SELECT spendlog_audit_id FROM selectedSpendlogAuditIds)),

  test_0 AS

    (SELECT "There is no overlap between offers refineinfoforwhite_id and refineinfoforyellow_id. Should be zero." as testCase,
            
             current_timestamp_var as testTimestamp,

             start_week_ts as testFromDate,

             end_week_ts as testToDate,

             count(main_tb.id) = 0 as testPassed,

             "RefineInfo id" as debugDataType,

             string_agg(CAST(main_tb.id as String)) as debugData

    FROM

    (SELECT offers_tb.refineinfoforwhite_id as id  FROM `dummy.dummy.offers` as offers_tb

    INNER JOIN `dummy.dummy.spendlog_audit` as spendlog_audit_tb

    ON offers_tb.spendlog_audit_id = spendlog_audit_tb.id

    WHERE offers_tb.id IN (SELECT offer_id FROM selectedOfferIds)


        AND offers_tb.refineinfoforwhite_id IN (SELECT refineinfoforyellow_id  FROM `dummy.dummy.offers` as offers_tb

                                                INNER JOIN `dummy.dummy.spendlog_audit` as spendlog_audit_tb

                                                ON offers_tb.spendlog_audit_id = spendlog_audit_tb.id

                                                WHERE offers_tb.id IN (SELECT offer_id FROM selectedOfferIds)
                                                      
                                                )
                                                
    UNION ALL


    SELECT offers_tb.refineinfoforyellow_id as id  FROM `dummy.dummy.offers` as offers_tb

    INNER JOIN `dummy.dummy.spendlog_audit` as spendlog_audit_tb

    ON offers_tb.spendlog_audit_id = spendlog_audit_tb.id

    WHERE spendlog_audit_tb.week_start_date >= PARSE_TIMESTAMP("%Y-%m-%d", "2020-12-26")
          AND spendlog_audit_tb.week_start_date <= PARSE_TIMESTAMP("%Y-%m-%d", "2021-01-29")


        AND offers_tb.refineinfoforyellow_id IN (SELECT refineinfoforwhite_id  FROM `dummy.dummy.offers` as offers_tb

                                                INNER JOIN `dummy.dummy.spendlog_audit` as spendlog_audit_tb

                                                ON offers_tb.spendlog_audit_id = spendlog_audit_tb.id

                                                WHERE spendlog_audit_tb.week_start_date >= PARSE_TIMESTAMP("%Y-%m-%d", "2020-12-26")
                                                      AND spendlog_audit_tb.week_start_date <= PARSE_TIMESTAMP("%Y-%m-%d", "2021-01-29")
                                                      
                                                )
    ) as main_tb





    
    
    
    
    ),


  test_1 AS
  
        (SELECT 
          "Offers that have white grams, but no refineinfoforwhite_id. Offers that have yellow grams, but no refineinfoforyellow_id. Should be zero." as testCase,

          current_timestamp_var as testTimestamp,
          
          start_week_ts as testFromDate,
          
          end_week_ts as testToDate,

          count(offers_tb.id) = 0 as testPassed,
          
          "Offer id" as debugDataType,

          string_agg(CAST(offers_tb.id as String)) as debugData


        FROM `dummy.dummy.offers` as offers_tb
        
        WHERE offers_tb.id IN (SELECT offer_id FROM selectedOfferIds)

              AND
              (
              (CASE WHEN offers_tb.wc_grams IS NULL THEN 0 ELSE offers_tb.wc_grams END
                 + CASE WHEN offers_tb.wd_grams IS NULL THEN 0 ELSE offers_tb.wd_grams END > 0 AND offers_tb.refineinfoforwhite_id IS NULL)

              OR (CASE WHEN offers_tb.yc_grams IS NULL THEN 0 ELSE offers_tb.yc_grams END
                 + CASE WHEN offers_tb.yd_grams IS NULL THEN 0 ELSE offers_tb.yd_grams END
                 + CASE WHEN offers_tb.yr_grams IS NULL THEN 0 ELSE offers_tb.yr_grams END > 0 AND offers_tb.refineinfoforyellow_id IS NULL)

              )

        ),
        
  selectedRefinedInfoIds AS
  
    (SELECT offers_tb.refineinfoforwhite_id as RefineInfo_id
        FROM `dummy.dummy.offers` as offers_tb
        WHERE offers_tb.id IN (SELECT offer_id FROM selectedOfferIds)
              AND offers_tb.refineinfoforwhite_id IS NOT NULL

        GROUP BY RefineInfo_id


        UNION ALL


        SELECT offers_tb.refineinfoforyellow_id as RefineInfo_id
        FROM `dummy.dummy.offers` as offers_tb
        WHERE offers_tb.id IN (SELECT offer_id FROM selectedOfferIds)
              AND offers_tb.refineinfoforyellow_id IS NOT NULL

        GROUP BY RefineInfo_id),
        
        
  test_2 AS 
  
      (SELECT "refineinfoforyellow_id and refineinfoforwhite_id from offers table which are not present in RefineInfo table. Should be zero" as testCase,
      
              current_timestamp_var as testTimestamp,
          
              start_week_ts as testFromDate,
          
              end_week_ts as testToDate,
              
              count(refineInfo_tb.id) = 0 AS testPassed,
              
              "RefineInfo id" AS debugDataType,
              
              string_agg(CAST(refineInfo_tb.id as String)) as debugData
      
      
      FROM
      
        selectedRefinedInfoIds AS main_tb
        
        LEFT JOIN `dummy.dummy.RefineInfo` as RefineInfo_tb
        
        ON main_tb.RefineInfo_id = RefineInfo_tb.id
        
        WHERE RefineInfo_tb.id IS NULL
      
      ),
      
      
        
  test_3 AS
  
    (SELECT "Records within Refine_Info table that have NULL melt_id. Should be zero." AS testCase,
            current_timestamp_var as testTimestamp,
            start_week_ts as testFromDate,
            end_week_ts as testToDate,
            
            count(RefineInfo_tb.id) = 0 as testPassed,
            
            "RefineInfo id" as debugDataType,
            
            string_agg(CAST(RefineInfo_tb.id as String)) as debugData
            
            

     
     FROM `dummy.dummy.RefineInfo` as RefineInfo_tb
     
     WHERE RefineInfo_tb.id IN (SELECT RefineInfo_id FROM selectedRefinedInfoIds)
            
           AND melt_id IS NULL
     
     ),
      
      
  selectedMeltIds AS
  
    (SELECT melt_id FROM `dummy.dummy.RefineInfo` as RefineInfo_tb
     WHERE RefineInfo_tb.id IN (SELECT RefineInfo_id FROM selectedRefinedInfoIds)
     GROUP BY melt_id
     ),
     
     
     
  test_4 AS
  
    (SELECT "Missing melt_ids within Melt table. Should be zero" AS testCase,
             current_timestamp_var as testTimestamp,
            start_week_ts as testFromDate,
            end_week_ts as testToDate,
            
            count(selectedMeltIds_tb.melt_id) = 0 as testPassed,
            
            "Melt id" as debugDataType,
            
            string_agg(CAST(selectedMeltIds_tb.melt_id as String)) as debugData
            
            
     FROM selectedMeltIds as selectedMeltIds_tb
     
     LEFT JOIN `dummy.dummy.Melt` as Melt_tb
     
     ON selectedMeltIds_tb.melt_id = Melt_tb.id
     
     WHERE Melt_tb.id IS NULL
    ),
    
    
  test_5 AS
  
    (SELECT "Records within Melt table that have NULL potnumber values. Should be Zero." as testCase,
    
     current_timestamp_var as testTimestamp,
     start_week_ts as testFromDate,
     end_week_ts as testToDate,
     
     count(melt_tb.id) = 0 AS testPassed,
     
     "Melt id" as debugDataType,
     
     string_agg(CAST(melt_tb.id as String)) as debugData
     
     
     FROM `dummy.dummy.Melt` as melt_tb
     
     WHERE melt_tb.id IN (SELECT melt_id FROM selectedMeltIds)
     
     AND melt_tb.potnumber IS NULL),
     
     
     
  test_6 AS
  
    (SELECT "Missing Potnumbers/Jobnumbers within Refinery_assay table. Should be zero." as testCase,
            
             current_timestamp_var as testTimestamp,
             
             start_week_ts as testFromDate,
             
             end_week_ts as testToDate,
             
             count(melt_tb.potnumber) = 0 AS testPassed,
             
             "potnumber" as debugDataType,
             
             string_agg(CAST(melt_tb.potnumber as String)) as debugData
             
             
     FROM `dummy.dummy.Melt` as melt_tb
     
     LEFT JOIN `dummy.dummy.rafinery_assay` as rafinery_assay_tb
     
     ON melt_tb.potnumber = rafinery_assay_tb.job_number__it
     
     WHERE melt_tb.id IN (SELECT melt_id FROM selectedMeltIds)
     
           AND rafinery_assay_tb.job_number__it IS NULL
            
 ),



 report_data AS

    (SELECT *
     FROM `dummy.dummy.REPORT_spendLogAudit` as REPORT_spendLogAudit_tb
     WHERE REPORT_spendLogAudit_tb.week_start_date >= start_week_ts
           AND REPORT_spendLogAudit_tb.week_end_date <= end_week_ts
    ),

test_7 AS

    (SELECT "Weekly Plan is populated for every single record. Should be zero." as testCase,

            current_timestamp_var as testTimestamp,

            start_week_ts as testFromDate,
             
            end_week_ts as testToDate,

            count(report_data_tb.rep_id) = 0 as testPassed,

            "Rep id: Name" as debugDataType,

            string_agg(DISTINCT CONCAT(CAST(report_data_tb.rep_id as String), ": ", CAST(report_data_tb.reps_name as String))) as debugData


    FROM report_data as report_data_tb

    WHERE report_data_tb.rep_id NOT IN (SELECT id FROM exceptionsRepId)
          AND
          (report_data_tb.plan_quota_weekly IS NULL
          OR report_data_tb.plan_quota_weekly <= 0)),



/* Sheet containing employment dates */
employment_dates AS

(SELECT * FROM 

    (SELECT CAST(reps_dates_tb.rep_id as INT64) AS rep_id,

                      PARSE_TIMESTAMP("%Y-%m-%d", CASE WHEN MIN(reps_dates_tb.start_date) IS NULL
                                                          OR MIN(reps_dates_tb.start_date) = ""
                                                      THEN "1900-01-01"
                                                      ELSE MIN(reps_dates_tb.start_date)
                                                  END
                      
                      ) as start_date_ts,



                      PARSE_TIMESTAMP("%Y-%m-%d", CASE WHEN MAX(reps_dates_tb.end_date) IS NULL
                                                            OR MAX(reps_dates_tb.end_date) = ""
                                                      THEN "3000-01-01"
                                                      ELSE MAX(reps_dates_tb.end_date)
                                                  END
                      
                      
                      
                      ) as end_date_ts,


                      MAX(reps_dates_tb.commission_assessment) as commission_assesment
          

                      FROM `dummy.rep_dats_sheet.reps_dates` as reps_dates_tb

                      WHERE reps_dates_tb.rep_id IS NOT NULL


              GROUP BY rep_id) as emp_dates_tb

  WHERE emp_dates_tb.start_date_ts != PARSE_TIMESTAMP("%Y-%m-%d", "3000-01-01")
),


test_8 AS

    (SELECT "All audited reps for the audited period do have employment records populated within 'reps_dates' sheet. Should be zero" as testCase,

      current_timestamp_var as testTimestamp,                       

      start_week_ts as testFromDate,
             
      end_week_ts as testToDate,

      count(main_tb.rep_id) = 0 as testPassed,

      "Rep id: Name" as debugDataType,

      string_agg(DISTINCT CONCAT(CAST(main_tb.rep_id as String), ": ", CAST(main_tb.reps_name as String))) as debugData

    

    FROM (SELECT rep_id, reps_name FROM report_data WHERE rep_id NOT IN (SELECT id FROM exceptionsRepId) GROUP BY rep_id, reps_name) as main_tb

    WHERE main_tb.rep_id NOT IN (SELECT rep_id FROM employment_dates GROUP BY rep_id)
    
    ),


test_9 AS

    (SELECT "BCR value is populated for every single rep. Should be zero." as testCase,

            current_timestamp_var as testTimestamp,

            start_week_ts as testFromDate,
             
            end_week_ts as testToDate,

            count(DISTINCT report_data_tb.rep_id) = 0 as testPassed,

            "Rep id: Name" as debugDataType,

            string_agg(DISTINCT CONCAT(CAST(report_data_tb.rep_id as String), ": ", CAST(report_data_tb.reps_name as String))) as debugData


    FROM report_data as report_data_tb

    WHERE report_data_tb.rep_id NOT IN (SELECT id FROM exceptionsRepId)
          AND
          (report_data_tb.reps_bcr_value IS NULL
          OR report_data_tb.reps_bcr_value <= 0)
          
    ),


test_10 AS

    (SELECT "Base pay value is populated for every single rep. Should be zero." as testCase,

            current_timestamp_var as testTimestamp,

            start_week_ts as testFromDate,
             
            end_week_ts as testToDate,

            count(DISTINCT report_data_tb.rep_id) = 0 as testPassed,

            "Rep id: Name" as debugDataType,

            string_agg(DISTINCT CONCAT(CAST(report_data_tb.rep_id as String), ": ", CAST(report_data_tb.reps_name as String))) as debugData


    FROM report_data as report_data_tb

    WHERE report_data_tb.rep_id NOT IN (SELECT id FROM exceptionsRepId)
          AND
          (report_data_tb.reps_base_pay_value IS NULL
          OR report_data_tb.reps_base_pay_value <= 0)
          
    ),


  test_11 AS

    (SELECT "Quarterly bonus values are populated for every single rep. Should be zero." as testCase,

            current_timestamp_var as testTimestamp,

            start_week_ts as testFromDate,
             
            end_week_ts as testToDate,

            count(DISTINCT report_data_tb.rep_id) = 0 as testPassed,

            "Rep id: Name" as debugDataType,

            string_agg(DISTINCT CONCAT(CAST(report_data_tb.rep_id as String), ": ", CAST(report_data_tb.reps_name as String))) as debugData


    FROM report_data as report_data_tb

    WHERE report_data_tb.rep_id NOT IN (SELECT id FROM exceptionsRepId)
          AND
          (report_data_tb.quarterly_bonus_tier_0_percent IS NULL
           OR report_data_tb.quarterly_bonus_tier_1_percent IS NULL
           OR report_data_tb.quarterly_bonus_tier_2_percent IS NULL
           OR report_data_tb.quarterly_bonus_tier_3_percent IS NULL
           OR report_data_tb.quarterly_bonus_tier_0_threshold IS NULL
           OR report_data_tb.quarterly_bonus_tier_1_threshold IS NULL
           OR report_data_tb.quarterly_bonus_tier_2_threshold IS NULL
           OR report_data_tb.quarterly_bonus_tier_3_threshold IS NULL
          )
          
    )



  
  /* Tests for DB integrity */
  SELECT * FROM test_0
  UNION ALL
  SELECT * FROM test_1
  UNION ALL
  SELECT * FROM test_2
  UNION ALL
  SELECT * FROM test_3
  UNION ALL
  SELECT * FROM test_4
  UNION ALL
  SELECT * FROM test_5
  UNION ALL
  SELECT * FROM test_6
  
  /* Tests for Report Integrity */
  UNION ALL
  SELECT * FROM test_7
  UNION ALL
  SELECT * FROM test_8
  UNION ALL
  SELECT * FROM test_9
  UNION ALL
  SELECT * FROM test_10
  UNION ALL
  SELECT * FROM test_11;


IF (SELECT count(*) FROM dummy.__TABLES__ WHERE table_id='AUDIT_REPORT_offersConsolidated') = 0 THEN
  CREATE TABLE IF NOT EXISTS `dummy.AUDIT_REPORT_offersConsolidated`
  AS
  SELECT * FROM TEMP_AUDIT_REPORT_offersConsolidated;

ELSE
  /* DELETE OLD DATA */
  /* DELETE FROM `dummy.AUDIT_offersConsolidated`
  WHERE True; */

  /* Load new data into a table */
  INSERT INTO `dummy.AUDIT_REPORT_offersConsolidated`
  SELECT * FROM TEMP_AUDIT_REPORT_offersConsolidated;


END IF;
END;