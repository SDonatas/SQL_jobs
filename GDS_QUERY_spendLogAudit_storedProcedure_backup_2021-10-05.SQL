CREATE OR REPLACE PROCEDURE `dummy.dummy.ETL_STAGING_spendLogAudit`()

BEGIN

/* Call stored procedures before running report */
CALL `dummy.dummy.ETL_TRANSFORM_reps_plan_quota`();



/* DATE SELECTOR IMPORT */
CREATE TEMP TABLE TEMP_dateSelector

AS

(SELECT start_date_ts,
        end_date_ts

 FROM `dummy.dummy.view_FIFO_not_closed_period`);


CREATE TEMP TABLE TEMP_dateSelectorYear

  AS

     SELECT   EXTRACT(
               Year FROM CASE WHEN EXTRACT(DAYOFWEEK FROM start_date_ts  ) = 1
                    THEN TIMESTAMP_ADD(start_date_ts  , INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts  ) = 2
                    THEN TIMESTAMP_ADD(start_date_ts  , INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts  ) = 3
                    THEN TIMESTAMP_ADD(start_date_ts , INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts  ) = 4
                    THEN TIMESTAMP_ADD(start_date_ts , INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts  ) = 5
                    THEN TIMESTAMP_ADD(start_date_ts , INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts  ) = 6
                    THEN start_date_ts 

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts  ) = 7
                    THEN TIMESTAMP_ADD(start_date_ts , INTERVAL 6 DAY)

                    ELSE NULL END) as start_year_ts,

          EXTRACT(Year FROM end_date_ts) as end_year_ts

          FROM TEMP_dateSelector;


/* RAW DATA SOURCE IMPORT WITH KEY FILTERING */


CREATE TEMP TABLE TEMP_spendlogAuditRaw

AS

 WITH spendlogAuditRaw AS

          (SELECT spendlog_audit_tb.*
          
           
           
           FROM  `dummy.dummy.spendlog_audit` as spendlog_audit_tb
          
           WHERE spendlog_audit_tb.week_start_date >= (SELECT start_date_ts FROM TEMP_dateSelector)

               AND spendlog_audit_tb.week_end_date <= (SELECT end_date_ts FROM TEMP_dateSelector)

               AND spendlog_audit_tb.melt_type = 'DDREP'),

     spendlogAuditRawMovementIn AS

          (/* Spend log moved items for assay later reporting */
          SELECT spendlog_audit_tb.*,
          
          
          "AssayMovementIn" as dataSource FROM  `dummy.dummy.spendlog_audit_assay_movements` as spendlog_audit_tb
          
           WHERE spendlog_audit_tb.week_start_date >= (SELECT start_date_ts FROM TEMP_dateSelector)

               AND spendlog_audit_tb.week_end_date <= (SELECT end_date_ts FROM TEMP_dateSelector)

               AND spendlog_audit_tb.melt_type = 'DDREP'
          
          ),

     /* Assumption movement can only be moved forward, not backward to past */
     spendlogAuditRawMovementOut AS

          (SELECT id, "AssayMovementOut" as dataSource
           FROM `dummy.dummy.spendlog_audit_assay_movements` as spendlog_audit_tb
           WHERE spendlog_audit_tb.week_start_date > (SELECT end_date_ts FROM TEMP_dateSelector))


     SELECT spendlogAuditRaw_tb.*,

            CASE WHEN spendlogAuditRawMovementOut_tb.dataSource IS NULL THEN NULL
                 ELSE spendlogAuditRawMovementOut_tb.dataSource
                 END as dataSource
     
     FROM spendlogAuditRaw as spendlogAuditRaw_tb

     LEFT JOIN spendlogAuditRawMovementOut AS spendlogAuditRawMovementOut_tb

     ON spendlogAuditRaw_tb.id = spendlogAuditRawMovementOut_tb.id

     UNION ALL

     SELECT * FROM spendlogAuditRawMovementIn;






CREATE TEMP TABLE TEMP_offersRaw

     AS

     WITH offersRaw AS

               (SELECT offers_tb.*,
                       refine_info_white.melt_id as refine_info_white_melt_id,
                       refine_info_yellow.melt_id as refine_info_yellow_melt_id
               
               
               FROM `dummy.dummy.offers` as offers_tb

               INNER JOIN TEMP_spendlogAuditRaw as spendlog_audit_tb

               ON offers_tb.spendlog_audit_id = spendlog_audit_tb.id

               LEFT JOIN `dummy.dummy.RefineInfo` as refine_info_white
               ON offers_tb.refineinfoforwhite_id = refine_info_white.id

               LEFT JOIN `dummy.dummy.RefineInfo` as refine_info_yellow
               ON offers_tb.refineinfoforyellow_id = refine_info_yellow.id

               /*
               
               WHERE spendlog_audit_tb.week_start_date >= (SELECT start_date_ts FROM TEMP_dateSelector)

                    AND spendlog_audit_tb.week_end_date <= (SELECT end_date_ts FROM TEMP_dateSelector)

               */

               )

          SELECT * FROM offersRaw;


/* FURTHER TRANSFORMATIONS */
CREATE TEMP TABLE TEMP_offers

AS

     WITH offers AS

          (/* Main audited offers */
           SELECT offers_tb.spendlog_audit_id,
                  
                  CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN spendlog_audit_tb.rep_id
                       ELSE offers_tb.rep_id
                       END AS rep_id,
                  
                  CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn'  THEN spendlog_audit_tb.territory_id
                       ELSE offers_tb.territory_id
                       END AS territory_id,
                  /*
                  CASE WHEN dataSource = 'AssayMovementIn' THEN spendlog_audit_tb.territory_id
                       ELSE offers_tb.territory_id
                       END AS territory_id, */

                  STRING_AGG(DISTINCT dataSource) as movements,
               
               /* METRICS FOR AUDITED VALUES */

               /* Offer metrics for all melt and non-melt */
               sum(CASE WHEN offers_tb.wc_grams IS NULL OR dataSource = 'AssayMovementIn' THEN 0 ELSE offers_tb.wc_grams END) as wc_grams,
               sum(CASE WHEN offers_tb.wd_grams IS NULL OR dataSource = 'AssayMovementIn' THEN 0 ELSE offers_tb.wd_grams END) as wd_grams,
               sum(CASE WHEN offers_tb.yc_grams IS NULL OR dataSource = 'AssayMovementIn' THEN 0 ELSE offers_tb.yc_grams END) as yc_grams,
               sum(CASE WHEN offers_tb.yd_grams IS NULL OR dataSource = 'AssayMovementIn' THEN 0 ELSE offers_tb.yd_grams END) as yd_grams,
               sum(CASE WHEN offers_tb.yr_grams IS NULL OR dataSource = 'AssayMovementIn' THEN 0 ELSE offers_tb.yr_grams END) as yr_grams,
               sum(CASE WHEN offers_tb.total_offer IS NULL OR dataSource = 'AssayMovementIn' THEN 0 ELSE offers_tb.total_offer END) as offers_total_offer,


               /* Offer Metrics for melt only */
               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementIn' THEN 0
                    ELSE CASE WHEN offers_tb.wc_grams IS NULL THEN 0 ELSE offers_tb.wc_grams END
                    END) as wc_grams_melt,

               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementIn' THEN 0
                    ELSE CASE WHEN offers_tb.wd_grams IS NULL THEN 0 ELSE offers_tb.wd_grams END
                    END) as wd_grams_melt,

               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementIn' THEN 0
                    ELSE CASE WHEN offers_tb.yc_grams IS NULL THEN 0 ELSE offers_tb.yc_grams END
                    END) as yc_grams_melt,

               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementIn' THEN 0
                    ELSE CASE WHEN offers_tb.yd_grams IS NULL THEN 0 ELSE offers_tb.yd_grams END
                    END) as yd_grams_melt,

               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementIn' THEN 0
                    ELSE CASE WHEN offers_tb.yr_grams IS NULL THEN 0 ELSE offers_tb.yr_grams END
                    END) as yr_grams_melt,

               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementIn' THEN 0
                    ELSE CASE WHEN offers_tb.total_offer IS NULL THEN 0 ELSE offers_tb.total_offer END
                    END) as offers_total_offer_melt,

               /* METRICS FOR ASSAYED VALUES */

               /* Offer metrics for all melt and non-melt */
               /*
               sum(CASE WHEN offers_tb.wc_grams IS NULL OR dataSource = 'AssayMovementOut' THEN 0 ELSE offers_tb.wc_grams END) as wc_grams_assayed,
               sum(CASE WHEN offers_tb.wd_grams IS NULL OR dataSource = 'AssayMovementOut' THEN 0 ELSE offers_tb.wd_grams END) as wd_grams_assayed,
               sum(CASE WHEN offers_tb.yc_grams IS NULL OR dataSource = 'AssayMovementOut' THEN 0 ELSE offers_tb.yc_grams END) as yc_grams_assayed,
               sum(CASE WHEN offers_tb.yd_grams IS NULL OR dataSource = 'AssayMovementOut' THEN 0 ELSE offers_tb.yd_grams END) as yd_grams_assayed,
               sum(CASE WHEN offers_tb.yr_grams IS NULL OR dataSource = 'AssayMovementOut' THEN 0 ELSE offers_tb.yr_grams END) as yr_grams_assayed, */
               sum(CASE WHEN offers_tb.total_offer IS NULL OR dataSource = 'AssayMovementOut'
                        THEN 0 ELSE offers_tb.total_offer END) AS offers_total_offer_assayed,


               /* Offer Metrics for melt only */
               /*
               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementOut' THEN 0
                    ELSE CASE WHEN offers_tb.wc_grams IS NULL THEN 0 ELSE offers_tb.wc_grams END
                    END) as wc_grams_melt_assayed,

               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementOut' THEN 0
                    ELSE CASE WHEN offers_tb.wd_grams IS NULL THEN 0 ELSE offers_tb.wd_grams END
                    END) as wd_grams_melt_assayed,

               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementOut' THEN 0
                    ELSE CASE WHEN offers_tb.yc_grams IS NULL THEN 0 ELSE offers_tb.yc_grams END
                    END) as yc_grams_melt_assayed,

               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementOut' THEN 0
                    ELSE CASE WHEN offers_tb.yd_grams IS NULL THEN 0 ELSE offers_tb.yd_grams END
                    END) as yd_grams_melt_assayed,

               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementOut' THEN 0
                    ELSE CASE WHEN offers_tb.yr_grams IS NULL THEN 0 ELSE offers_tb.yr_grams END
                    END) as yr_grams_melt_assayed, */

               sum(CASE WHEN offers_tb.melt_id IS NULL OR dataSource = 'AssayMovementOut' THEN 0
                    ELSE CASE WHEN offers_tb.total_offer IS NULL THEN 0 ELSE offers_tb.total_offer END
                    END) as offers_total_offer_melt_assayed



          
               FROM TEMP_offersRaw as offers_tb

               INNER JOIN TEMP_spendlogAuditRaw as spendlog_audit_tb

               ON offers_tb.spendlog_audit_id = spendlog_audit_tb.id
               

               /*
               WHERE spendlog_audit_tb.week_start_date >= (SELECT start_date_ts FROM TEMP_dateSelector)

                    AND spendlog_audit_tb.week_end_date <= (SELECT end_date_ts FROM TEMP_dateSelector)

                    AND spendlog_audit_tb.melt_type = 'DDREP'
               */

               GROUP BY spendlog_audit_id,
                        rep_id,
                        territory_id
                        /* spendlog_audit_tb.dataSource */
          
          )

     SELECT * FROM offers;



CREATE TEMP TABLE TEMP_offersToMeltLinkEnriched

AS

WITH offersToMeltLink AS

          (SELECT offersRaw_tb.melt_id as melt_id,
                    offersRaw_tb.rep_id as rep_id,
                    offersRaw_tb.territory_id as territory_id,
                    spendlogAuditRaw_tb.week_start_date as week_start_date,
                    spendlogAuditRaw_tb.week_end_date as week_end_date,

                    SUM(CASE WHEN offersRaw_tb.melt_product = "white"
                         
                         THEN    CASE WHEN spendlogAuditRaw_tb.wc_a IS NULL THEN 0 ELSE spendlogAuditRaw_tb.wc_a END
                                   + CASE WHEN spendlogAuditRaw_tb.wc_ar IS NULL THEN 0 ELSE spendlogAuditRaw_tb.wc_ar END
                                   + CASE WHEN spendlogAuditRaw_tb.wd_a IS NULL THEN 0 ELSE spendlogAuditRaw_tb.wd_a END
                                   + CASE WHEN spendlogAuditRaw_tb.wd_ar IS NULL THEN 0 ELSE spendlogAuditRaw_tb.wd_ar END
                         
                         ELSE 0 END) as white_audit_grams,

                    SUM(CASE WHEN offersRaw_tb.melt_product = "yellow"
                         
                         THEN CASE WHEN spendlogAuditRaw_tb.yc_a IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yc_a END
                              + CASE WHEN spendlogAuditRaw_tb.yc_ar IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yc_ar END
                              + CASE WHEN spendlogAuditRaw_tb.yd_a IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yd_a END
                              + CASE WHEN spendlogAuditRaw_tb.yd_ar IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yd_ar END
                              + CASE WHEN spendlogAuditRaw_tb.yr_a IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yr_a END
                              + CASE WHEN spendlogAuditRaw_tb.yr_ar IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yr_ar END
                         
                         ELSE 0 END) as yellow_audit_grams,


                    SUM(CASE WHEN offersRaw_tb.melt_product = "white"
                         
                         THEN    CASE WHEN spendlogAuditRaw_tb.wc_a IS NULL THEN 0 ELSE spendlogAuditRaw_tb.wc_a END
                                   + CASE WHEN spendlogAuditRaw_tb.wc_ar IS NULL THEN 0 ELSE spendlogAuditRaw_tb.wc_ar END
                                   + CASE WHEN spendlogAuditRaw_tb.wd_a IS NULL THEN 0 ELSE spendlogAuditRaw_tb.wd_a END
                                   + CASE WHEN spendlogAuditRaw_tb.wd_ar IS NULL THEN 0 ELSE spendlogAuditRaw_tb.wd_ar END

                         WHEN offersRaw_tb.melt_product = "yellow"

                         THEN CASE WHEN spendlogAuditRaw_tb.yc_a IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yc_a END
                              + CASE WHEN spendlogAuditRaw_tb.yc_ar IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yc_ar END
                              + CASE WHEN spendlogAuditRaw_tb.yd_a IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yd_a END
                              + CASE WHEN spendlogAuditRaw_tb.yd_ar IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yd_ar END
                              + CASE WHEN spendlogAuditRaw_tb.yr_a IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yr_a END
                              + CASE WHEN spendlogAuditRaw_tb.yr_ar IS NULL THEN 0 ELSE spendlogAuditRaw_tb.yr_ar END
                                   
                         ELSE 0 END  
                                   
                                   
                                   
                                   ) AS total_audit_grams


                    /* 
                    SUM(spendlogAuditRaw_tb.wc_a
                    + spendlogAuditRaw_tb.wc_ar
                    + spendlogAuditRaw_tb.wd_a
                    + spendlogAuditRaw_tb.wd_ar
                    + spendlogAuditRaw_tb.yc_a
                    + spendlogAuditRaw_tb.yc_ar
                    + spendlogAuditRaw_tb.yd_a
                    + spendlogAuditRaw_tb.yd_ar
                    + spendlogAuditRaw_tb.yr_a
                    + spendlogAuditRaw_tb.yr_ar)
                    AS total_audit_grams_white_and_yellow
                    */

          FROM (SELECT spendlog_audit_id,
                       refine_info_white_melt_id as melt_id,
                       "white" as melt_product,
                       rep_id,
                       territory_id

               FROM TEMP_offersRaw

               WHERE refine_info_white_melt_id IS NOT NULL

               /* TEMP solution exclude items for assay that have been moved elsewhere */
               AND spendlog_audit_id NOT IN (SELECT id FROM TEMP_spendlogAuditRaw WHERE dataSource = 'AssayMovementOut')

               
               GROUP BY spendlog_audit_id,
                        melt_id,
                        rep_id,
                        territory_id

               UNION ALL

               SELECT spendlog_audit_id,
                       refine_info_yellow_melt_id as melt_id,
                       "yellow" as melt_product,
                       rep_id,
                       territory_id

               FROM TEMP_offersRaw

               WHERE refine_info_yellow_melt_id IS NOT NULL

               /* TEMP solution exclude items for assay that have been moved elsewhere */
               AND spendlog_audit_id NOT IN (SELECT id FROM TEMP_spendlogAuditRaw WHERE dataSource = 'AssayMovementOut')
               
               GROUP BY spendlog_audit_id,
                        melt_id,
                        rep_id,
                        territory_id
               
               ) as offersRaw_tb

          INNER JOIN TEMP_spendlogAuditRaw as spendlogAuditRaw_tb

          ON offersRaw_tb.spendlog_audit_id = spendlogAuditRaw_tb.id

          GROUP BY melt_id, rep_id, territory_id, week_start_date, week_end_date)

          /* Assumption above one spendLogAudit_id to melt_id is one to one relationship */ 

     SELECT offersToMeltLink_tb.melt_id as melt_id,
            offersToMeltLink_tb.rep_id as rep_id,
            offersToMeltLink_tb.territory_id as territory_id,
            offersToMeltLink_tb.week_start_date as week_start_date,
            offersToMeltLink_tb.week_end_date as week_end_date,
            offersToMeltLink_tb.yellow_audit_grams as yellow_audit_grams,
            offersToMeltLink_tb.white_audit_grams as white_audit_grams,
            offersToMeltLink_tb.total_audit_grams as total_audit_grams,

            /* Totals per melt */
            offersToMeltLinkTotalPerMelt_tb.total_audit_grams_per_melt_id as total_audit_grams_per_melt_id,

            /* Percetnages */
            offersToMeltLink_tb.yellow_audit_grams
            / offersToMeltLinkTotalPerMelt_tb.total_audit_grams_per_melt_id AS yellow_audit_grams_percent,

            offersToMeltLink_tb.white_audit_grams
            / offersToMeltLinkTotalPerMelt_tb.total_audit_grams_per_melt_id AS white_audit_grams_percent,

            offersToMeltLink_tb.total_audit_grams
            / offersToMeltLinkTotalPerMelt_tb.total_audit_grams_per_melt_id AS total_audit_grams_percent


           FROM offersToMeltLink as offersToMeltLink_tb

           LEFT JOIN (SELECT offersToMeltLink_tb.melt_id as melt_id,
                             SUM(offersToMeltLink_tb.total_audit_grams) as total_audit_grams_per_melt_id

                      FROM offersToMeltLink as offersToMeltLink_tb
                      
                      GROUP BY melt_id
                      ) as offersToMeltLinkTotalPerMelt_tb

          ON offersToMeltLink_tb.melt_id = offersToMeltLinkTotalPerMelt_tb.melt_id;

/* RAW MELTS IMPORT */

CREATE TEMP TABLE TEMP_rawMelts

AS

     (SELECT melt_tb.id as melt_id,
               melt_tb.potnumber as melt_potnumber,
               melt_tb.refinery as melt_refinery,

               melt_tb.assaypdweight as melt_assaypdweight,
               melt_tb.assayptweight as melt_assayptweight,
               melt_tb.assayauweight as melt_assayauweight,
               melt_tb.assayagweight as melt_assayagweight,
               
               CASE WHEN melt_tb.assaypdweight IS NULL THEN 0 ELSE melt_tb.assaypdweight END
               + CASE WHEN melt_tb.assayptweight IS NULL THEN 0 ELSE melt_tb.assayptweight END
               + CASE WHEN melt_tb.assayauweight IS NULL THEN 0 ELSE melt_tb.assayauweight END
               + CASE WHEN melt_tb.assayagweight IS NULL THEN 0 ELSE melt_tb.assayagweight END as melt_assayTotalWeight,


               melt_tb.assaypdvalue as melt_assaypdvalue,
               melt_tb.assayptvalue as melt_assayptvalue,
               melt_tb.assayauvalue as melt_assayauvalue,
               melt_tb.assayagvalue as melt_assayagvalue

               /*
               rafinery_assay_tb.ag_grams as rafinery_assay_ag_grams,
               rafinery_assay_tb.au_grams as rafinery_assay_au_grams,
               rafinery_assay_tb.pd_grams as rafinery_assay_pd_grams,
               rafinery_assay_tb.pt_grams as rafinery_assay_pt_grams,

               rafinery_assay_tb.pt_dollar_per_gram as rafinery_assay_pt_dollar_per_gram,
               rafinery_assay_tb.pd_dollar_per_gram as rafinery_assay_pd_dollar_per_gram,
               rafinery_assay_tb.au_dollar_per_gram as rafinery_assay_au_dollar_per_gram,
               rafinery_assay_tb.ag_dollar_per_gram as rafinery_assay_ag_dollar_per_gram,

               rafinery_assay_tb.pt_dollars as rafinery_assay_pt_dollars,
               rafinery_assay_tb.pd_dollars as rafinery_assay_pd_dollars,
               rafinery_assay_tb.au_dollars as rafinery_assay_au_dollars,
               rafinery_assay_tb.ag_dollars as rafinery_assay_ag_dollars */


          FROM `dummy.dummy.Melt` as melt_tb

          INNER JOIN (SELECT melt_id FROM TEMP_offersToMeltLinkEnriched GROUP BY melt_id) as offersToMeltLinkEnriched_tb

          ON melt_tb.id = offersToMeltLinkEnriched_tb.melt_id
          
          /* LEFT JOIN `dummy.dummy.rafinery_assay` as rafinery_assay_tb

          ON CAST(melt_tb.potnumber as String) = rafinery_assay_tb.job_number */

     );


CREATE TEMP TABLE TEMP_refineryAssayEnhanced

AS
/*
WITH meltPots AS

          (SELECT melt_potnumber AS melt_potnumber
               
          FROM TEMP_rawMelts
               
          GROUP BY melt_potnumber),


     meltPotsEnriched AS

          (SELECT melt_potnumber, CAST(melt_potnumber as String) as melt_potnumber_refinery_map FROM meltPots

           UNION ALL

           SELECT melt_potnumber, CONCAT(CAST(melt_potnumber as String), 'A') as melt_potnumber_refinery_map FROM meltPots

           UNION ALL

           SELECT melt_potnumber, CONCAT(CAST(melt_potnumber as String), 'AB') as melt_potnumber_refinery_map FROM meltPots

           UNION ALL

           SELECT melt_potnumber, CONCAT(CAST(melt_potnumber AS String), 'ABC') as melt_potnumber_refinery_map FROM meltPots

           UNION ALL

           SELECT melt_potnumber, CONCAT(CAST(melt_potnumber AS String), 'ABCD') as melt_potnumber_refinery_map FROM meltPots

           UNION ALL

           SELECT melt_potnumber, CONCAT(CAST(melt_potnumber AS String), 'ABCDE') as melt_potnumber_refinery_map FROM meltPots

           UNION ALL

           SELECT melt_potnumber, CONCAT(CAST(melt_potnumber AS String), 'ABCDEF') as melt_potnumber_refinery_map FROM meltPots

           UNION ALL

           SELECT melt_potnumber, CONCAT(CAST(melt_potnumber AS String), 'ABCDEFG') as melt_potnumber_refinery_map FROM meltPots)

*/
/*     refineryAssay AS */

          SELECT rafinery_assay.*,
                 rafinery_assay.job_number__it as melt_potnumber,
                 assay_params_tb.*
          
          FROM `dummy.dummy.rafinery_assay` AS rafinery_assay

          /*
          INNER JOIN meltPotsEnriched as meltPotsEnriched_tb

          ON rafinery_assay.job_number = meltPotsEnriched_tb.melt_potnumber_refinery_map */

          
          /* Join parameters for DDREP */
          CROSS JOIN (SELECT * FROM

                    (SELECT AVG(CAST(param_value AS float64)) as ag_high_ni_percent
                    FROM `dummy.dummy.assay_params` as assay_terms_tb
                    WHERE melt_type = 'DDREP' and param_name = 'ag_high_ni_percent'
                          AND (start_ts >= (SELECT start_date_ts FROM TEMP_dateSelector)
                               AND (end_ts <= (SELECT end_date_ts FROM TEMP_dateSelector) OR end_ts IS NULL)
                              )
                    
                    
                    )

                    CROSS JOIN

                    (SELECT AVG(CAST(param_value AS float64)) as ag_low_ni_percent
                    FROM `dummy.dummy.assay_params` as assay_terms_tb
                    WHERE melt_type = 'DDREP' and param_name = 'ag_low_ni_percent'
                          AND (start_ts >= (SELECT start_date_ts FROM TEMP_dateSelector)
                               AND (end_ts <= (SELECT end_date_ts FROM TEMP_dateSelector) OR end_ts IS NULL)
                              )                    
                    
                    
                    )

                    CROSS JOIN

                    (SELECT AVG(CAST(param_value AS float64)) as au_high_ni_percent
                    FROM `dummy.dummy.assay_params` as assay_terms_tb
                    WHERE melt_type = 'DDREP' and param_name = 'au_high_ni_percent'
                          AND (start_ts >= (SELECT start_date_ts FROM TEMP_dateSelector)
                               AND (end_ts <= (SELECT end_date_ts FROM TEMP_dateSelector) OR end_ts IS NULL)
                              )                    
                    )

                    CROSS JOIN

                    (SELECT AVG(CAST(param_value AS float64)) as au_low_ni_percent
                    FROM `dummy.dummy.assay_params` as assay_terms_tb
                    WHERE melt_type = 'DDREP' and param_name = 'au_low_ni_percent'
                          AND (start_ts >= (SELECT start_date_ts FROM TEMP_dateSelector)
                               AND (end_ts <= (SELECT end_date_ts FROM TEMP_dateSelector) OR end_ts IS NULL)
                              )                    
                    )


                    CROSS JOIN

                    (SELECT AVG(CAST(param_value AS float64)) as pd_high_ni_percent
                    FROM `dummy.dummy.assay_params` as assay_terms_tb
                    WHERE melt_type = 'DDREP' and param_name = 'pd_high_ni_percent'
                          AND (start_ts >= (SELECT start_date_ts FROM TEMP_dateSelector)
                               AND (end_ts <= (SELECT end_date_ts FROM TEMP_dateSelector) OR end_ts IS NULL)
                              )                    
                    )

                    CROSS JOIN

                    (SELECT AVG(CAST(param_value AS float64)) as pd_low_ni_percent
                    FROM `dummy.dummy.assay_params` as assay_terms_tb
                    WHERE melt_type = 'DDREP' and param_name = 'pd_low_ni_percent'
                          AND (start_ts >= (SELECT start_date_ts FROM TEMP_dateSelector)
                               AND (end_ts <= (SELECT end_date_ts FROM TEMP_dateSelector) OR end_ts IS NULL)
                              )                    
                    )

                    CROSS JOIN

                    (SELECT AVG(CAST(param_value AS float64)) as pt_high_ni_percent
                    FROM `dummy.dummy.assay_params` as assay_terms_tb
                    WHERE melt_type = 'DDREP' and param_name = 'pt_high_ni_percent'
                          AND (start_ts >= (SELECT start_date_ts FROM TEMP_dateSelector)
                               AND (end_ts <= (SELECT end_date_ts FROM TEMP_dateSelector) OR end_ts IS NULL)
                              )                    
                    )

                    CROSS JOIN

                    (SELECT AVG(CAST(param_value AS float64)) as pt_low_ni_percent
                    FROM `dummy.dummy.assay_params` as assay_terms_tb
                    WHERE melt_type = 'DDREP' and param_name = 'pt_low_ni_percent'
                          AND (start_ts >= (SELECT start_date_ts FROM TEMP_dateSelector)
                               AND (end_ts <= (SELECT end_date_ts FROM TEMP_dateSelector) OR end_ts IS NULL)
                              )                    
                    )

                    ) as assay_params_tb;

          

                    


/*     
     refineryAssaySettlementPrices AS


          (SELECT dim_calendar_tb.date AS date,
                 
                 AVG(xigdatas_AG_tb.value) / 31.1035 AS AG_value,
                 AVG(xigdatas_AU_tb.value) / 31.1035 AS AU_value,
                 AVG(xigdatas_PD_tb.value) / 31.1035 AS PD_value,
                 AVG(xigdatas_PT_tb.value) / 31.1035 AS PT_value
          
          FROM


          (SELECT date FROM  `dummy.dummy.dim_calendar` as dim_calendar_tb

           WHERE date BETWEEN (SELECT TIMESTAMP_SUB(min(settlement_date), INTERVAL 7 DAY) FROM refineryAssay)

              AND (SELECT max(settlement_date) FROM refineryAssay)
                    
          ) as dim_calendar_tb


          /* AG */
          /*
          LEFT JOIN  `dummy.dummy.xigdatas` AS xigdatas_AG_tb

               ON dim_calendar_tb.date = xigdatas_AG_tb.date
                    AND xigdatas_AG_tb.currency = 'USD' AND xigdatas_AG_tb.symbol = 'AG' AND xigdatas_AG_tb.value IS NOT NULL AND xigdatas_AG_tb.value > 0

          */
          /* AU */
          /*
          LEFT JOIN  `dummy.dummy.xigdatas` AS xigdatas_AU_tb

               ON dim_calendar_tb.date = xigdatas_AU_tb.date
                    AND xigdatas_AU_tb.currency = 'USD' AND xigdatas_AU_tb.symbol = 'AU' AND xigdatas_AU_tb.value IS NOT NULL AND xigdatas_AU_tb.value > 0


          */
          /* PD */
          /*
          LEFT JOIN  `dummy.dummy.xigdatas` AS xigdatas_PD_tb

               ON dim_calendar_tb.date = xigdatas_PD_tb.date
                    AND xigdatas_PD_tb.currency = 'USD' AND xigdatas_PD_tb.symbol = 'PD' AND xigdatas_PD_tb.value IS NOT NULL AND xigdatas_PD_tb.value > 0


          */
           /* PT */
           /*
          LEFT JOIN  `dummy.dummy.xigdatas` AS xigdatas_PT_tb

               ON dim_calendar_tb.date = xigdatas_PT_tb.date
                    AND xigdatas_PT_tb.currency = 'USD' AND xigdatas_PT_tb.symbol = 'PT' AND xigdatas_PT_tb.value IS NOT NULL AND xigdatas_PT_tb.value > 0



          GROUP BY date)

          */

     /*
     SELECT refineryAssay_tb.*,
            
                  CASE WHEN refineryAssaySettlementPrices_tb.AG_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_tb.AG_value

                       WHEN refineryAssaySettlementPrices_offset_1_tb.AG_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_1_tb.AG_value

                       WHEN refineryAssaySettlementPrices_offset_2_tb.AG_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_2_tb.AG_value

                       WHEN refineryAssaySettlementPrices_offset_3_tb.AG_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_3_tb.AG_value

                       WHEN refineryAssaySettlementPrices_offset_4_tb.AG_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_4_tb.AG_value

                       WHEN refineryAssaySettlementPrices_offset_5_tb.AG_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_5_tb.AG_value

                       WHEN refineryAssaySettlementPrices_offset_6_tb.AG_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_6_tb.AG_value

                       WHEN refineryAssaySettlementPrices_offset_7_tb.AG_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_7_tb.AG_value
                 END as AG_value,


                 CASE WHEN refineryAssaySettlementPrices_tb.AU_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_tb.AU_value

                       WHEN refineryAssaySettlementPrices_offset_1_tb.AU_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_1_tb.AU_value

                       WHEN refineryAssaySettlementPrices_offset_2_tb.AU_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_2_tb.AU_value

                       WHEN refineryAssaySettlementPrices_offset_3_tb.AU_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_3_tb.AU_value

                       WHEN refineryAssaySettlementPrices_offset_4_tb.AU_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_4_tb.AU_value

                       WHEN refineryAssaySettlementPrices_offset_5_tb.AU_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_5_tb.AU_value

                       WHEN refineryAssaySettlementPrices_offset_6_tb.AU_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_6_tb.AU_value

                       WHEN refineryAssaySettlementPrices_offset_7_tb.AU_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_7_tb.AU_value
                 END as AU_value,



                 CASE WHEN refineryAssaySettlementPrices_tb.PD_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_tb.PD_value

                       WHEN refineryAssaySettlementPrices_offset_1_tb.PD_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_1_tb.PD_value

                       WHEN refineryAssaySettlementPrices_offset_2_tb.PD_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_2_tb.PD_value

                       WHEN refineryAssaySettlementPrices_offset_3_tb.PD_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_3_tb.PD_value

                       WHEN refineryAssaySettlementPrices_offset_4_tb.PD_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_4_tb.PD_value

                       WHEN refineryAssaySettlementPrices_offset_5_tb.PD_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_5_tb.PD_value

                       WHEN refineryAssaySettlementPrices_offset_6_tb.PD_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_6_tb.PD_value

                       WHEN refineryAssaySettlementPrices_offset_7_tb.PD_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_7_tb.PD_value
                 END as PD_value,


                 CASE WHEN refineryAssaySettlementPrices_tb.PT_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_tb.PT_value

                       WHEN refineryAssaySettlementPrices_offset_1_tb.PT_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_1_tb.PT_value

                       WHEN refineryAssaySettlementPrices_offset_2_tb.PT_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_2_tb.PT_value

                       WHEN refineryAssaySettlementPrices_offset_3_tb.PT_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_3_tb.PT_value

                       WHEN refineryAssaySettlementPrices_offset_4_tb.PT_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_4_tb.PT_value

                       WHEN refineryAssaySettlementPrices_offset_5_tb.PT_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_5_tb.PT_value

                       WHEN refineryAssaySettlementPrices_offset_6_tb.PT_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_6_tb.PT_value

                       WHEN refineryAssaySettlementPrices_offset_7_tb.PT_value IS NOT NULL
                       THEN refineryAssaySettlementPrices_offset_7_tb.PT_value
                 END as PT_value



           FROM refineryAssay as refineryAssay_tb
          
          */
          /* Join on exact day */
          /*
           LEFT JOIN refineryAssaySettlementPrices AS refineryAssaySettlementPrices_tb

           ON refineryAssay_tb.settlement_date = refineryAssaySettlementPrices_tb.date
          
          */
           /* Join on offset by 1 day back */
           /*
           LEFT JOIN refineryAssaySettlementPrices AS refineryAssaySettlementPrices_offset_1_tb

           ON refineryAssay_tb.settlement_date = TIMESTAMP_ADD(refineryAssaySettlementPrices_offset_1_tb.date, INTERVAL 1 DAY)

          */
           /* Join on offset by 2 day back */
           /*
           LEFT JOIN refineryAssaySettlementPrices AS refineryAssaySettlementPrices_offset_2_tb

           ON refineryAssay_tb.settlement_date = TIMESTAMP_ADD(refineryAssaySettlementPrices_offset_2_tb.date, INTERVAL 2 DAY)

          */
           /* Join on offset by 3 day back */
           /*
           LEFT JOIN refineryAssaySettlementPrices AS refineryAssaySettlementPrices_offset_3_tb

           ON refineryAssay_tb.settlement_date = TIMESTAMP_ADD(refineryAssaySettlementPrices_offset_3_tb.date, INTERVAL 3 DAY)

          */
            /* Join on offset by 4 day back */
            /*
           LEFT JOIN refineryAssaySettlementPrices AS refineryAssaySettlementPrices_offset_4_tb

           ON refineryAssay_tb.settlement_date = TIMESTAMP_ADD(refineryAssaySettlementPrices_offset_4_tb.date, INTERVAL 4 DAY)

          */
           /* Join on offset by 5 day back */
           /*
           LEFT JOIN refineryAssaySettlementPrices AS refineryAssaySettlementPrices_offset_5_tb

           ON refineryAssay_tb.settlement_date = TIMESTAMP_ADD(refineryAssaySettlementPrices_offset_5_tb.date, INTERVAL 5 DAY)

          */
           /* Join on offset by 6 day back */
           /*
           LEFT JOIN refineryAssaySettlementPrices AS refineryAssaySettlementPrices_offset_6_tb

           ON refineryAssay_tb.settlement_date = TIMESTAMP_ADD(refineryAssaySettlementPrices_offset_6_tb.date, INTERVAL 6 DAY)

          */
           /* Join on offset by 7 day back */
           /*
           LEFT JOIN refineryAssaySettlementPrices AS refineryAssaySettlementPrices_offset_7_tb

           ON refineryAssay_tb.settlement_date = TIMESTAMP_ADD(refineryAssaySettlementPrices_offset_7_tb.date, INTERVAL 7 DAY);

          */

CREATE TEMP TABLE TEMP_meltsAllocated

AS
     
WITH rawMeltsEnhanced as

          (SELECT 
                  /* Dimensions */
                  rawMelts_tb.melt_id AS melt_id,
                  rawMelts_tb.melt_potnumber,
                  rawMelts_tb.melt_refinery,
                  
                  /* Melt weights */
                  rawMelts_tb.melt_assaypdweight as melt_assaypdweight,
                  rawMelts_tb.melt_assayptweight as melt_assayptweight,
                  rawMelts_tb.melt_assayauweight as melt_assayauweight,
                  rawMelts_tb.melt_assayagweight as melt_assayagweight,

                  rawMelts_tb.melt_assayTotalWeight as melt_assayTotalWeight,
                  (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as melt_assayPercentSharePerPot,

                  /* Melt au, ag, pd, pt values */
                  rawMelts_tb.melt_assaypdvalue as melt_assaypdvalue,
                  rawMelts_tb.melt_assayptvalue as melt_assayptvalue,
                  rawMelts_tb.melt_assayauvalue as melt_assayauvalue,
                  rawMelts_tb.melt_assayagvalue as melt_assayagvalue,

                  /* Refinery assayed settlement prices without terms */
                  /*
                  rafinery_assay_tb.AG_value as AG_value,
                  rafinery_assay_tb.AU_value as AU_value,
                  rafinery_assay_tb.PD_value as PD_value,
                  rafinery_assay_tb.PT_value as PT_value,
                  */


                  /* Refinery grams */
                  rafinery_assay_tb.ag_grams * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_ag_grams,
                  rafinery_assay_tb.au_grams * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_au_grams,
                  rafinery_assay_tb.pd_grams * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_pd_grams,
                  rafinery_assay_tb.pt_grams * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_pt_grams,

                  /* Refinery actual price per gram au, ag, pd, pt including terms */
                  rafinery_assay_tb.ag_dollar_per_gram as rafinery_assay_ag_dollar_per_gram,
                  rafinery_assay_tb.au_dollar_per_gram as rafinery_assay_au_dollar_per_gram,
                  rafinery_assay_tb.pd_dollar_per_gram as rafinery_assay_pd_dollar_per_gram,
                  rafinery_assay_tb.pt_dollar_per_gram as rafinery_assay_pt_dollar_per_gram,
                  
                  /* Refinery assay total price with terms */
                  rafinery_assay_tb.pt_dollars * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_pt_dollars,
                  rafinery_assay_tb.pd_dollars * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_pd_dollars,
                  rafinery_assay_tb.au_dollars * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_au_dollars,
                  rafinery_assay_tb.ag_dollars * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_ag_dollars,


                  rafinery_assay_tb.assay_fee * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_fee,
                  rafinery_assay_tb.market_value_at_fix * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_market_value_at_fix,


                  rafinery_assay_tb.pt_dollars * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) AS rafinery_assay_pt_dollars_without_terms,
                  rafinery_assay_tb.pd_dollars * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) AS rafinery_assay_pd_dollars_without_terms,
                  rafinery_assay_tb.au_dollars * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) AS rafinery_assay_au_dollars_without_terms,
                  rafinery_assay_tb.ag_dollars * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) AS rafinery_assay_ag_dollars_without_terms



                  /* Refinery asay total price without terms */
                  /*
                  (rafinery_assay_tb.pt_dollars
                    * (CASE WHEN rafinery_assay_tb.ni_percent >= 25
                           THEN rafinery_assay_tb.pt_high_ni_percent
                           ELSE rafinery_assay_tb.pt_low_ni_percent
                      END) / 100) * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_pt_dollars_without_terms,

                  (rafinery_assay_tb.pd_dollars 
                  * (CASE WHEN rafinery_assay_tb.ni_percent >= 25
                         THEN rafinery_assay_tb.pd_high_ni_percent
                         ELSE rafinery_assay_tb.pd_low_ni_percent
                    END) / 100) * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_pd_dollars_without_terms,

                  (rafinery_assay_tb.au_dollars
                  * (CASE WHEN rafinery_assay_tb.ni_percent >= 25
                         THEN rafinery_assay_tb.au_high_ni_percent
                         ELSE rafinery_assay_tb.au_low_ni_percent
                    END) / 100) * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_au_dollars_without_terms,


                  (rafinery_assay_tb.ag_dollars
                  * (CASE WHEN rafinery_assay_tb.ni_percent >= 25
                         THEN rafinery_assay_tb.ag_high_ni_percent
                         ELSE rafinery_assay_tb.ag_low_ni_percent
                    END) / 100) * (rawMelts_tb.melt_assayTotalWeight / rawMeltsPerPot.melt_assayTotalWeightPerPot) as rafinery_assay_ag_dollars_without_terms

                   */

           FROM TEMP_rawMelts as rawMelts_tb

           LEFT JOIN TEMP_refineryAssayEnhanced as rafinery_assay_tb

           ON rawMelts_tb.melt_potnumber = rafinery_assay_tb.melt_potnumber

           LEFT JOIN (SELECT rawMelts_tb.melt_potnumber as melt_potnumber,
                              SUM(CASE WHEN rawMelts_tb.melt_assayTotalWeight IS NULL THEN 0 ELSE rawMelts_tb.melt_assayTotalWeight END) as melt_assayTotalWeightPerPot
     

                       FROM TEMP_rawMelts as rawMelts_tb

                       GROUP BY melt_potnumber) as rawMeltsPerPot

          ON rawMelts_tb.melt_potnumber = rawMeltsPerPot.melt_potnumber
           
           
           )



    SELECT                                                  
          
          /* Dimensions */
          offersToMeltLinkEnriched_tb.rep_id as rep_id,
          offersToMeltLinkEnriched_tb.territory_id as territory_id,
          offersToMeltLinkEnriched_tb.week_start_date as week_start_date,
          offersToMeltLinkEnriched_tb.week_end_date as week_end_date,
          
          melt_tb.melt_id as melt_id,
          melt_tb.melt_potnumber as melt_potnumber,
          melt_tb.melt_refinery as melt_refinery,

          offersToMeltLinkEnriched_tb.total_audit_grams_percent as total_audit_grams_percent,
          melt_tb.melt_assayPercentSharePerPot as melt_assayPercentSharePerPot,

          /* DEBUG Metrics - Audit Grams purely for drilldown reports */
          SUM(offersToMeltLinkEnriched_tb.yellow_audit_grams) as audit_yellow_grams,
          SUM(offersToMeltLinkEnriched_tb.white_audit_grams) as audit_white_grams,
          SUM(offersToMeltLinkEnriched_tb.total_audit_grams) as audit_total_grams,


          /* DEBUG Metrics for melt and refinery */
          SUM(melt_tb.melt_assaypdweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as melt_assaypdweight,
          SUM(melt_tb.melt_assayptweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as melt_assayptweight,
          SUM(melt_tb.melt_assayauweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as melt_assayauweight,
          SUM(melt_tb.melt_assayagweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as melt_assayagweight,
          
          /* Melt Metrics */
          SUM( (melt_tb.melt_assaypdweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) * dummy.bigDecimal_division(CAST(melt_tb.melt_assaypdvalue AS BigDecimal), CAST(31.1035 AS BigDecimal)) ) AS melt_assay_pd_market_value,
          SUM( (melt_tb.melt_assayptweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) * dummy.bigDecimal_division(CAST(melt_tb.melt_assayptvalue AS BigDecimal), CAST(31.1035 AS BigDecimal)) ) AS melt_assay_pt_market_value,
          SUM( (melt_tb.melt_assayauweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) * dummy.bigDecimal_division(CAST(melt_tb.melt_assayauvalue AS BigDecimal), CAST(31.1035 AS BigDecimal)) ) AS melt_assay_au_market_value,
          SUM( (melt_tb.melt_assayagweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) * dummy.bigDecimal_division(CAST(melt_tb.melt_assayagvalue AS BigDecimal), CAST(31.1035 AS BigDecimal)) ) AS melt_assay_ag_market_value,

          /* Refinery Metrics */
          SUM(melt_tb.rafinery_assay_ag_grams * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_ag_grams,
          SUM(melt_tb.rafinery_assay_au_grams * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_au_grams,
          SUM(melt_tb.rafinery_assay_pd_grams * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_pd_grams,
          SUM(melt_tb.rafinery_assay_pt_grams * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_pt_grams,

          AVG(melt_tb.rafinery_assay_ag_dollar_per_gram) as rafinery_assay_ag_dollar_per_gram_avg,
          AVG(melt_tb.rafinery_assay_au_dollar_per_gram) as rafinery_assay_au_dollar_per_gram_avg,
          AVG(melt_tb.rafinery_assay_pd_dollar_per_gram) as rafinery_assay_pd_dollar_per_gram_avg,
          AVG(melt_tb.rafinery_assay_pt_dollar_per_gram) as rafinery_assay_pt_dollar_per_gram_avg,

          /*
          AVG(melt_tb.AG_value) AS rafinery_assay_AG_dollar_per_gram_without_terms_avg,
          AVG(melt_tb.AU_value) AS rafinery_assay_AU_dollar_per_gram_without_terms_avg,
          AVG(melt_tb.PD_value) AS rafinery_assay_PD_dollar_per_gram_without_terms_avg,
          AVG(melt_tb.PT_value) AS rafinery_assay_PT_dollar_per_gram_without_terms_avg,
          */

          SUM(melt_tb.rafinery_assay_fee * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_fee,
          SUM(melt_tb.rafinery_market_value_at_fix * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_market_value_at_fix,

          SUM(melt_tb.rafinery_assay_pt_dollars * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_pt_dollars,
          SUM(melt_tb.rafinery_assay_pd_dollars * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_pd_dollars,
          SUM(melt_tb.rafinery_assay_au_dollars * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_au_dollars,
          SUM(melt_tb.rafinery_assay_ag_dollars * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_ag_dollars,

          SUM(melt_tb.rafinery_assay_pt_dollars_without_terms * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_pt_dollars_without_terms,
          SUM(melt_tb.rafinery_assay_pd_dollars_without_terms * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_pd_dollars_without_terms,
          SUM(melt_tb.rafinery_assay_au_dollars_without_terms * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_au_dollars_without_terms,
          SUM(melt_tb.rafinery_assay_ag_dollars_without_terms * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as rafinery_assay_ag_dollars_without_terms




          /* SUM(melt_tb.measuredweightwithbags * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as melt_measuredweightwithbags, */
          /* SUM(melt_tb.assayweightloss * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as melt_assayweightloss, */
          /* SUM(melt_tb.achievedagweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as melt_achievedagweight, */
          /* SUM(melt_tb.meltfees * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as melt_meltfees, */
          /* SUM(melt_tb.achievedptvalue * offersToMeltLinkEnriched_tb.total_audit_grams_percent) as melt_achievedptvalue, */
          /*
          SUM(melt_tb.assayagvalue) as melt_assayagvalue,
          SUM(melt_tb.assaypdvalue) as melt_assaypdvalue,
          SUM(melt_tb.assayptvalue) as melt_assayptvalue,
          SUM(melt_tb.assayauvalue) AS melt_assayauvalue,
                                                            */
          /* SUM(melt_tb.achievednonpreciousweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) AS melt_achievednonpreciousweight,
          SUM(melt_tb.achievedauweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) AS melt_achievedauweight,
          SUM(melt_tb.achievedptweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) AS melt_achievedptweight,
          SUM(melt_tb.assayfees * offersToMeltLinkEnriched_tb.total_audit_grams_percent) AS melt_assayfees,
          
          SUM(melt_tb.achievedpdweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) AS melt_achievedpdweight,
          SUM(melt_tb.achievedagvalue * offersToMeltLinkEnriched_tb.total_audit_grams_percent) AS melt_achievedagvalue,
          SUM(melt_tb.achievedpdvalue * offersToMeltLinkEnriched_tb.total_audit_grams_percent) AS melt_achievedpdvalue,
          
          SUM(melt_tb.measuredbagsweight * offersToMeltLinkEnriched_tb.total_audit_grams_percent) AS melt_measuredbagsweight, */
          /* melt_tb.repexp AS melt_repexp */
          /* melt_tb.month AS melt_month */
          /* melt_tb.locked AS melt_locked */
          /* melt_tb.spend AS melt_spend */
          
          /* SUM(melt_tb.achievedauvalue * offersToMeltLinkEnriched_tb.total_audit_grams_percent) AS melt_achievedauvalue,
          SUM(melt_tb.shippingfees * offersToMeltLinkEnriched_tb.total_audit_grams_percent) AS melt_shippingfees, */
          /* melt_tb.description AS melt_description */
          
          
     

     FROM rawMeltsEnhanced as melt_tb

     INNER JOIN TEMP_offersToMeltLinkEnriched as offersToMeltLinkEnriched_tb

     ON melt_tb.melt_id = offersToMeltLinkEnriched_tb.melt_id

     GROUP BY rep_id,
               territory_id,
               week_start_date,
               week_end_date,
               melt_id,
               melt_potnumber,
               melt_refinery,
               total_audit_grams_percent,
               melt_assayPercentSharePerPot;


CREATE TEMP TABLE TEMP_meltsRefined

AS

SELECT /* Dimensions */
          rep_id,
          territory_id,
          week_start_date,
          week_end_date,
          
          STRING_AGG(CAST(melt_id as String)) as melt_ids,
          STRING_AGG(CAST(melt_potnumber as String)) as melt_potnumber,
          STRING_AGG(CAST(melt_refinery as String)) as melt_refinery,

          /* total_audit_grams_percent,
          melt_assayPercentSharePerPot, */

          /* DEBUG Metrics for melt and refinery */
          SUM(melt_assaypdweight) as melt_assaypdweight,
          SUM(melt_assayptweight) as melt_assayptweight,
          SUM(melt_assayauweight) as melt_assayauweight,
          SUM(melt_assayagweight) as melt_assayagweight,
          
          /* Melt Metrics */
          SUM(melt_assay_pd_market_value) as melt_assay_pd_market_value,
          SUM(melt_assay_pt_market_value) as melt_assay_pt_market_value,
          SUM(melt_assay_au_market_value) as melt_assay_au_market_value,
          SUM(melt_assay_ag_market_value) as melt_assay_ag_market_value,

          /* Refinery Metrics */
          SUM(rafinery_assay_ag_grams) as rafinery_assay_ag_grams,
          SUM(rafinery_assay_au_grams) as rafinery_assay_au_grams,
          SUM(rafinery_assay_pd_grams) as rafinery_assay_pd_grams,
          SUM(rafinery_assay_pt_grams) as rafinery_assay_pt_grams,

          SUM(rafinery_assay_ag_dollar_per_gram_avg) as rafinery_assay_ag_dollar_per_gram_avg,
          SUM(rafinery_assay_au_dollar_per_gram_avg) as rafinery_assay_au_dollar_per_gram_avg,
          SUM(rafinery_assay_pd_dollar_per_gram_avg) as rafinery_assay_pd_dollar_per_gram_avg,
          SUM(rafinery_assay_pt_dollar_per_gram_avg) as rafinery_assay_pt_dollar_per_gram_avg,


          SUM(rafinery_assay_fee) as rafinery_assay_fee,
          SUM(rafinery_market_value_at_fix) as rafinery_market_value_at_fix,

          SUM(rafinery_assay_pt_dollars) as rafinery_assay_pt_dollars,
          SUM(rafinery_assay_pd_dollars) as rafinery_assay_pd_dollars,
          SUM(rafinery_assay_au_dollars) as rafinery_assay_au_dollars,
          SUM(rafinery_assay_ag_dollars) as rafinery_assay_ag_dollars,

          SUM(rafinery_assay_pt_dollars_without_terms) as rafinery_assay_pt_dollars_without_terms,
          SUM(rafinery_assay_pd_dollars_without_terms) as rafinery_assay_pd_dollars_without_terms,
          SUM(rafinery_assay_au_dollars_without_terms) as rafinery_assay_au_dollars_without_terms,
          SUM(rafinery_assay_ag_dollars_without_terms) as rafinery_assay_ag_dollars_without_terms



FROM TEMP_meltsAllocated

GROUP BY rep_id,
         territory_id,
         week_start_date,
         week_end_date;



CREATE TEMP TABLE TEMP_offersAuditEnriched

AS


WITH offersAudit AS
     /* Purchase Orders */

     (SELECT offers_tb.rep_id AS offers_rep_id,
             offers_tb.territory_id AS offers_territory_id,

             spendlog_audit_tb.week_start_date as offer_date_start_week,
             spendlog_audit_tb.week_end_date as offer_date_end_week,

             string_agg(DISTINCT offers_tb.movements) as movements,

             /*
               CASE WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 1 THEN CAST(CAST(TIMESTAMP_SUB(offer_date, INTERVAL 1 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 2 THEN CAST(CAST(TIMESTAMP_SUB(offer_date, INTERVAL 2 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 3 THEN CAST(CAST(TIMESTAMP_SUB(offer_date, INTERVAL 3 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 4 THEN CAST(CAST(TIMESTAMP_SUB(offer_date, INTERVAL 4 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 5 THEN CAST(CAST(TIMESTAMP_SUB(offer_date, INTERVAL 5 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 6 THEN CAST(CAST(TIMESTAMP_SUB(offer_date, INTERVAL 6 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 7 THEN CAST(CAST(offer_date as Date) as Timestamp)

               ELSE NULL END as offer_date_start_week,

               CASE WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 1 THEN CAST(CAST(TIMESTAMP_ADD(offer_date, INTERVAL 5 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 2 THEN CAST(CAST(TIMESTAMP_ADD(offer_date, INTERVAL 4 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 3 THEN CAST(CAST(TIMESTAMP_ADD(offer_date, INTERVAL 3 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 4 THEN CAST(CAST(TIMESTAMP_ADD(offer_date, INTERVAL 2 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 5 THEN CAST(CAST(TIMESTAMP_ADD(offer_date, INTERVAL 1 DAY) as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 6 THEN CAST(CAST(offer_date as Date) as Timestamp)
                    WHEN EXTRACT(DAYOFWEEK FROM offer_date) = 7 THEN CAST(CAST(TIMESTAMP_ADD(offer_date, INTERVAL 6 DAY) as Date) as Timestamp)

               ELSE NULL END as offer_date_end_week,

            */
               

               /* AUDIT METRICS */
               /* Offer metrics for all melt and non-melt */
               SUM(offers_tb.wc_grams) as wc_grams,
               SUM(offers_tb.wd_grams) as wd_grams,
               SUM(offers_tb.yc_grams) as yc_grams,
               SUM(offers_tb.yd_grams) as yd_grams,
               SUM(offers_tb.yr_grams) as yr_grams,
               SUM(offers_tb.offers_total_offer) as offers_total_offer,

               /* Offer Metrics for melt only */
               SUM(offers_tb.wc_grams_melt) as wc_grams_melt,
               SUM(offers_tb.wd_grams_melt) as wd_grams_melt,
               SUM(offers_tb.yc_grams_melt) as yc_grams_melt,
               SUM(offers_tb.yd_grams_melt) as yd_grams_melt,
               SUM(offers_tb.yr_grams_melt) as yr_grams_melt,
               SUM(offers_tb.offers_total_offer_melt) as offers_total_offer_melt,

               /* Audit metrics */
               SUM(CAST(CASE WHEN spendlog_audit_tb.nps_m IS NULL THEN 0 ELSE spendlog_audit_tb.nps_m END as float64)) as nps_m,
               SUM(CAST(CASE WHEN spendlog_audit_tb.nps_o IS NULL THEN 0 ELSE spendlog_audit_tb.nps_o END as float64)) as nps_o,
               SUM(CAST(CASE WHEN spendlog_audit_tb.wc_a IS NULL THEN 0 ELSE spendlog_audit_tb.wc_a END as float64)) as wc_a,
               SUM(CAST(CASE WHEN spendlog_audit_tb.wc_ar IS NULL THEN 0 ELSE spendlog_audit_tb.wc_ar END as float64)) as wc_ar,
               SUM(CAST(CASE WHEN spendlog_audit_tb.wd_a IS NULL THEN 0 ELSE spendlog_audit_tb.wd_a END as float64)) as wd_a,
               SUM(CAST(CASE WHEN spendlog_audit_tb.wd_ar IS NULL THEN 0 ELSE spendlog_audit_tb.wd_ar END as float64)) as wd_ar,
               SUM(CAST(CASE WHEN spendlog_audit_tb.yc_a IS NULL THEN 0 ELSE spendlog_audit_tb.yc_a END as float64)) as yc_a,
               SUM(CAST(CASE WHEN spendlog_audit_tb.yc_ar IS NULL THEN 0 ELSE spendlog_audit_tb.yc_ar END as float64)) as yc_ar,
               SUM(CAST(CASE WHEN spendlog_audit_tb.yd_a IS NULL THEN 0 ELSE spendlog_audit_tb.yd_a END as float64)) as yd_a, 
               SUM(CAST(CASE WHEN spendlog_audit_tb.yd_ar IS NULL THEN 0 ELSE spendlog_audit_tb.yd_ar END as float64)) as yd_ar,
               SUM(CAST(CASE WHEN spendlog_audit_tb.yr_a IS NULL THEN 0 ELSE spendlog_audit_tb.yr_a END as float64)) as yr_a,
               SUM(CAST(CASE WHEN spendlog_audit_tb.yr_ar IS NULL THEN 0 ELSE spendlog_audit_tb.yr_ar END as float64)) as yr_ar,


               /* ASSAYED METRICS */
               /* Offer metrics for all melt and non-melt */
               /*
               SUM(offers_tb.wc_grams_assayed) as wc_grams_assayed,
               SUM(offers_tb.wd_grams_assayed) as wd_grams_assayed,
               SUM(offers_tb.yc_grams_assayed) as yc_grams_assayed,
               SUM(offers_tb.yd_grams_assayed) as yd_grams_assayed,
               SUM(offers_tb.yr_grams_assayed) as yr_grams_assayed, */
               SUM(offers_tb.offers_total_offer_assayed) as offers_total_offer_assayed,

               /* Offer Metrics for melt only */
               /*
               SUM(offers_tb.wc_grams_melt_assayed) as wc_grams_melt_assayed,
               SUM(offers_tb.wd_grams_melt_assayed) as wd_grams_melt_assayed,
               SUM(offers_tb.yc_grams_melt_assayed) as yc_grams_melt_assayed,
               SUM(offers_tb.yd_grams_melt_assayed) as yd_grams_melt_assayed,
               SUM(offers_tb.yr_grams_melt_assayed) as yr_grams_melt_assayed, */
               SUM(offers_tb.offers_total_offer_melt_assayed) as offers_total_offer_melt_assayed

               /* Audit metrics */
               /*
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.nps_m as float64) END) as nps_m_assayed,
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.nps_o as float64) END) as nps_o_assayed,
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.wc_a as float64) END) as wc_a_assayed,
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.wc_ar as float64) END) as wc_ar_assayed,
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.wd_a as float64) END) as wd_a_assayed,
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.wd_ar as float64) END) as wd_ar_assayed,
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.yc_a as float64) END) as yc_a_assayed,
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.yc_ar as float64) END) as yc_ar_assayed,
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.yd_a as float64) END) as yd_a_assayed, 
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.yd_ar as float64) END) as yd_ar_assayed,
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.yr_a as float64) END) as yr_a_assayed,
               SUM(CASE WHEN spendlog_audit_tb.dataSource = 'AssayMovementIn' THEN 0 ELSE CAST(spendlog_audit_tb.yr_ar as float64) END) as yr_ar_assayed */



     FROM TEMP_offers as offers_tb

     INNER JOIN TEMP_spendlogAuditRaw as spendlog_audit_tb
     ON offers_tb.spendlog_audit_id = spendlog_audit_tb.id

     /* WHERE */
     /* PARAMETERS 
          spendlog_audit_tb.week_start_date >= (SELECT start_date_ts FROM TEMP_dateSelector)

          AND spendlog_audit_tb.week_end_date <= (SELECT end_date_ts FROM TEMP_dateSelector) */
     

     GROUP BY offers_rep_id,
               offers_territory_id,
               offer_date_start_week,
               offer_date_end_week)


     SELECT offersAudit_tb.*,
            offersAuditTerritoryCount_tb.offers_territory_id_count as offers_territory_id_count
           
           
           FROM offersAudit as offersAudit_tb

           LEFT JOIN (SELECT offers_rep_id,
                             offer_date_start_week,
                             offer_date_end_week,
                             sum(CASE WHEN offers_territory_id IS NULL THEN 1 ELSE 1 END) as offers_territory_id_count
           
                      FROM offersAudit AS offersAudit_tb

                      GROUP BY offers_rep_id,
                               offer_date_start_week,
                               offer_date_end_week) as offersAuditTerritoryCount_tb
          
          ON offersAudit_tb.offers_rep_id = offersAuditTerritoryCount_tb.offers_rep_id
             AND offersAudit_tb.offer_date_start_week = offersAuditTerritoryCount_tb.offer_date_start_week
             AND offersAudit_tb.offer_date_end_week = offersAuditTerritoryCount_tb.offer_date_end_week;



CREATE TEMP TABLE TEMP_base_pay

AS

     (SELECT rep_id,
               year,
               1 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "1")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "2")), INTERVAL 1 DAY) as end_date_ts, */
               january_mid + january_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)



     UNION ALL

     SELECT rep_id,
               year,
               2 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "2")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "3")), INTERVAL 1 DAY) as end_date_ts, */
               february_mid + february_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)

     UNION ALL

     SELECT rep_id,
               year,
               3 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "3")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "4")), INTERVAL 1 DAY) as end_date_ts, */
               march_mid + march_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)

     UNION ALL

     SELECT rep_id,
               year,
               4 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "4")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "5")), INTERVAL 1 DAY) as end_date_ts, */
               april_mid + april_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)

     UNION ALL

     SELECT rep_id,
               year,
               5 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "5")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "6")), INTERVAL 1 DAY) as end_date_ts, */
               may_mid + may_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)

     UNION ALL

     SELECT rep_id,
               year,
               6 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "6")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "7")), INTERVAL 1 DAY) as end_date_ts, */
               june_mid + june_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)


     UNION ALL

     SELECT rep_id,
               year,
               7 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "7")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "8")), INTERVAL 1 DAY) as end_date_ts, */
               july_mid + july_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)

     UNION ALL

     SELECT rep_id,
               year,
               8 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "8")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "9")), INTERVAL 1 DAY) as end_date_ts, */
               august_mid + august_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)

     UNION ALL

     SELECT rep_id,
               year,
               9 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "9")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "10")), INTERVAL 1 DAY) as end_date_ts, */
               september_mid + september_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)

     UNION ALL

     SELECT rep_id,
               year,
               10 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "10")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "11")), INTERVAL 1 DAY) as end_date_ts, */
               october_mid + october_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)


     UNION ALL

     SELECT rep_id,
               year,
               11 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "11")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "12")), INTERVAL 1 DAY) as end_date_ts, */
               november_mid + november_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)


     UNION ALL

     SELECT rep_id,
               year,
               12 as month,
               /* PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year AS String), "12")) as start_date_ts, */
               /* TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m", CONCAT(CAST(year + 1 AS String), "1")), INTERVAL 1 DAY) as end_date_ts, */
               december_mid + december_end as base_pay

     FROM `dummy.rep_dats_sheet.reps_base_pay`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)

     );   


CREATE TEMP TABLE TEMP_bcr_value

AS

     (SELECT  rep_id,
               year,
               1 as month,
               january_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)
          
     UNION ALL 

     SELECT rep_id,
          year,
          2 as month,
          february_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)
          
     UNION ALL 

     SELECT rep_id,
          year,
          3 as month,
          march_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)


     UNION ALL 

     SELECT rep_id,
          year,
          4 as month,
          april_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)


     UNION ALL 

     SELECT rep_id,
          year,
          5 as month,
          may_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)


     UNION ALL 

     SELECT rep_id,
          year,
          6 as month,
          june_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)
          
          
     UNION ALL 

     SELECT rep_id,
          year,
          7 as month,
          july_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)
          
     UNION ALL 

     SELECT rep_id,
          year,
          8 as month,
          august_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)
          

     UNION ALL 

     SELECT rep_id,
          year,
          9 as month,
          september_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)
          
     UNION ALL 

     SELECT rep_id,
          year,
          10 as month,
          october_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)

     UNION ALL

     SELECT rep_id,
          year,
          11 as month,
          november_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)
          
          
     UNION ALL

     SELECT rep_id,
          year,
          12 as month,
          december_value as bcr_value

     FROM `dummy.rep_dats_sheet.reps_bcr`

     WHERE year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
          AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)
     );





CREATE TEMP TABLE LoadTable_REPORT_spendLogAudit

AS

WITH dateDimension AS

          (SELECT week_start_ts,
                  week_end_ts,
                  wk_week_start_ts,
                  wk_week_end_ts,
                  audit_year as year_num,
                  audit_month as month_num

          
           FROM `dummy.dummy.dim_calendar` as dim_calendar_tb
           
           WHERE week_start_ts >= (SELECT start_date_ts FROM TEMP_dateSelector)
                AND week_end_ts <= (SELECT end_date_ts FROM TEMP_dateSelector)

           GROUP BY week_start_ts,
                    week_end_ts,
                    wk_week_start_ts,
                    wk_week_end_ts,
                    year_num,
                    month_num),


     dateDimensionEnriched AS

          (SELECT dateDimension_tb.*,
                  dateDimensionWeekCount_tb.weekCount


           FROM dateDimension as dateDimension_tb

           
           LEFT JOIN (SELECT year_num,
                             month_num,
                             count(week_start_ts) as weekCount

                      FROM dateDimension

                      GROUP BY year_num,
                               month_num

                     ) as dateDimensionWeekCount_tb


          ON dateDimension_tb.year_num = dateDimensionWeekCount_tb.year_num
            AND dateDimension_tb.month_num = dateDimensionWeekCount_tb.month_num
          
          ),


      reps_dates AS

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


           GROUP BY rep_id
           
           ),


     



     repsDimension AS

          (SELECT rep_id
           
           FROM `dummy.dummy.reps` as reps_tb

           INNER JOIN reps_dates as reps_dates_tb

           ON reps_tb.id = reps_dates_tb.rep_id

           WHERE (SELECT end_date_ts FROM TEMP_dateSelector) >= reps_dates_tb.start_date_ts
                 AND (SELECT start_date_ts FROM TEMP_dateSelector) <= reps_dates_tb.end_date_ts

                
          GROUP BY rep_id),



     plan_quota AS

          (SELECT plan_quota_tb.rep_id as rep_id,
                  plan_quota_tb.year as year,
                  plan_quota_tb.week_start_ts as week_start_ts,
                  plan_quota_tb.week_end_ts as week_end_ts,
                  /* CASE WHEN reps_dates_tb.rep_id IS NULL THEN 0 ELSE plan_quota END as plan_quota */
                  plan_quota_tb.plan_quota

          FROM `dummy.dummy.transformed_reps_plan_quota` as plan_quota_tb

          LEFT JOIN reps_dates as reps_dates_tb
          ON plan_quota_tb.rep_id = reps_dates_tb.rep_id

             AND (plan_quota_tb.wk_week_start_ts >= reps_dates_tb.start_date_ts OR reps_dates_tb.start_date_ts IS NULL)
             AND (plan_quota_tb.wk_week_end_ts <= reps_dates_tb.end_date_ts OR reps_dates_tb.end_date_ts IS NULL)


          WHERE plan_quota_tb.rep_id IN (SELECT rep_id FROM repsDimension)
                AND plan_quota_tb.year BETWEEN (SELECT start_year_ts FROM TEMP_dateSelectorYear)
                                        AND (SELECT end_year_ts FROM TEMP_dateSelectorYear)
                
          
          /*
          plan_quota_tb.week_start_ts >= (SELECT start_date_ts FROM dateSelector)
               AND plan_quota_tb.week_end_ts <= (SELECT end_date_ts FROM dateSelector)

          */

          ),


     plan_quota_yearly AS

          (SELECT plan_quota_tb.year as year,
                  plan_quota_tb.rep_id as rep_id,
                  SUM(plan_quota_tb.plan_quota) as plan_quota


           FROM plan_quota as plan_quota_tb

           GROUP BY year,
                    rep_id),





    xigdatas_AG as

    (SELECT date as xigdatas_date_ts,

           /* week end date */
          CASE WHEN EXTRACT(DAYOFWEEK FROM date) = 1
          THEN TIMESTAMP_ADD(date , INTERVAL 5 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 2
          THEN TIMESTAMP_ADD(date , INTERVAL 4 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 3
          THEN TIMESTAMP_ADD(date, INTERVAL 3 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 4
          THEN TIMESTAMP_ADD(date, INTERVAL 2 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 5
          THEN TIMESTAMP_ADD(date, INTERVAL 1 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 6
          THEN date

          WHEN EXTRACT(DAYOFWEEK FROM date ) = 7
          THEN TIMESTAMP_ADD(date, INTERVAL 6 DAY)

          ELSE NULL END as week_end_date_ts,


          /* Difference between current date and week end date */
          DATE_DIFF(

          CASE WHEN EXTRACT(DAYOFWEEK FROM date) = 1
          THEN TIMESTAMP_ADD(date , INTERVAL 5 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 2
          THEN TIMESTAMP_ADD(date , INTERVAL 4 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 3
          THEN TIMESTAMP_ADD(date, INTERVAL 3 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 4
          THEN TIMESTAMP_ADD(date, INTERVAL 2 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 5
          THEN TIMESTAMP_ADD(date, INTERVAL 1 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 6
          THEN date

          WHEN EXTRACT(DAYOFWEEK FROM date ) = 7
          THEN TIMESTAMP_ADD(date, INTERVAL 6 DAY)

          ELSE NULL END,

          date,
          DAY) +  CASE WHEN period = 'Morning' THEN 0.2
               WHEN period = 'Evening' THEN 0.1
               ELSE 0.2 END as week_end_date_offset,


           /* End of week date */
           MAX(value) as xigdatas_value_AG,

           /*
           MAX(CASE WHEN symbol = 'AG' THEN value ELSE NULL END) as xigdatas_value_AG
           MAX(CASE WHEN symbol = 'AU' THEN value ELSE NULL END) as xigdatas_value_AU,
           MAX(CASE WHEN symbol = 'PD' THEN value ELSE NULL END) as xigdatas_value_PD,
           MAX(CASE WHEN symbol = 'PT' THEN value ELSE NULL END) as xigdatas_value_PT
           */

           FROM `dummy.dummy.xigdatas`

           WHERE currency = 'USD' AND symbol = 'AG' AND value IS NOT NULL

               AND date BETWEEN (SELECT start_date_ts FROM TEMP_dateSelector)

               AND (SELECT end_date_ts FROM TEMP_dateSelector)

           GROUP BY xigdatas_date_ts, week_end_date_ts, week_end_date_offset

           ),
       
       
       
    xigdatas_AU as

    (SELECT date as xigdatas_date_ts,

           /* week end date */
          CASE WHEN EXTRACT(DAYOFWEEK FROM date) = 1
          THEN TIMESTAMP_ADD(date , INTERVAL 5 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 2
          THEN TIMESTAMP_ADD(date , INTERVAL 4 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 3
          THEN TIMESTAMP_ADD(date, INTERVAL 3 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 4
          THEN TIMESTAMP_ADD(date, INTERVAL 2 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 5
          THEN TIMESTAMP_ADD(date, INTERVAL 1 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 6
          THEN date

          WHEN EXTRACT(DAYOFWEEK FROM date ) = 7
          THEN TIMESTAMP_ADD(date, INTERVAL 6 DAY)

          ELSE NULL END as week_end_date_ts,


          /* Difference between current date and week end date */
          DATE_DIFF(

          CASE WHEN EXTRACT(DAYOFWEEK FROM date) = 1
          THEN TIMESTAMP_ADD(date , INTERVAL 5 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 2
          THEN TIMESTAMP_ADD(date , INTERVAL 4 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 3
          THEN TIMESTAMP_ADD(date, INTERVAL 3 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 4
          THEN TIMESTAMP_ADD(date, INTERVAL 2 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 5
          THEN TIMESTAMP_ADD(date, INTERVAL 1 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 6
          THEN date

          WHEN EXTRACT(DAYOFWEEK FROM date ) = 7
          THEN TIMESTAMP_ADD(date, INTERVAL 6 DAY)

          ELSE NULL END,

          date,
          DAY) +  CASE WHEN period = 'Morning' THEN 0.2
               WHEN period = 'Evening' THEN 0.1
               ELSE 0.2 END as week_end_date_offset,


           /* End of week date */
           MAX(value) as xigdatas_value_AU,

           /*
           MAX(CASE WHEN symbol = 'AG' THEN value ELSE NULL END) as xigdatas_value_AG
           MAX(CASE WHEN symbol = 'AU' THEN value ELSE NULL END) as xigdatas_value_AU,
           MAX(CASE WHEN symbol = 'PD' THEN value ELSE NULL END) as xigdatas_value_PD,
           MAX(CASE WHEN symbol = 'PT' THEN value ELSE NULL END) as xigdatas_value_PT
           */

           FROM `dummy.dummy.xigdatas`

           WHERE currency = 'USD' AND symbol = 'AU' AND value IS NOT NULL

           AND date BETWEEN (SELECT start_date_ts FROM TEMP_dateSelector)

             AND (SELECT end_date_ts FROM TEMP_dateSelector)


           GROUP BY xigdatas_date_ts, week_end_date_ts, week_end_date_offset

           ),
           
           
                      
    xigdatas_PD as

    (SELECT date as xigdatas_date_ts,

           /* week end date */
          CASE WHEN EXTRACT(DAYOFWEEK FROM date) = 1
          THEN TIMESTAMP_ADD(date , INTERVAL 5 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 2
          THEN TIMESTAMP_ADD(date , INTERVAL 4 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 3
          THEN TIMESTAMP_ADD(date, INTERVAL 3 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 4
          THEN TIMESTAMP_ADD(date, INTERVAL 2 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 5
          THEN TIMESTAMP_ADD(date, INTERVAL 1 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 6
          THEN date

          WHEN EXTRACT(DAYOFWEEK FROM date ) = 7
          THEN TIMESTAMP_ADD(date, INTERVAL 6 DAY)

          ELSE NULL END as week_end_date_ts,


          /* Difference between current date and week end date */
          DATE_DIFF(

          CASE WHEN EXTRACT(DAYOFWEEK FROM date) = 1
          THEN TIMESTAMP_ADD(date , INTERVAL 5 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 2
          THEN TIMESTAMP_ADD(date , INTERVAL 4 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 3
          THEN TIMESTAMP_ADD(date, INTERVAL 3 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 4
          THEN TIMESTAMP_ADD(date, INTERVAL 2 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 5
          THEN TIMESTAMP_ADD(date, INTERVAL 1 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 6
          THEN date

          WHEN EXTRACT(DAYOFWEEK FROM date ) = 7
          THEN TIMESTAMP_ADD(date, INTERVAL 6 DAY)

          ELSE NULL END,

          date,
          DAY) +  CASE WHEN period = 'Morning' THEN 0.2
               WHEN period = 'Evening' THEN 0.1
               ELSE 0.2 END as week_end_date_offset,


           /* End of week date */
           MAX(value) as xigdatas_value_PD,

           /*
           MAX(CASE WHEN symbol = 'AG' THEN value ELSE NULL END) as xigdatas_value_AG
           MAX(CASE WHEN symbol = 'AU' THEN value ELSE NULL END) as xigdatas_value_AU,
           MAX(CASE WHEN symbol = 'PD' THEN value ELSE NULL END) as xigdatas_value_PD,
           MAX(CASE WHEN symbol = 'PT' THEN value ELSE NULL END) as xigdatas_value_PT
           */

           FROM `dummy.dummy.xigdatas`

           WHERE currency = 'USD' AND symbol = 'PD' AND value IS NOT NULL

                 AND date BETWEEN (SELECT start_date_ts FROM TEMP_dateSelector)

             AND (SELECT end_date_ts FROM TEMP_dateSelector)


           GROUP BY xigdatas_date_ts, week_end_date_ts, week_end_date_offset

           ),
           
           
  xigdatas_PT as

    (SELECT date as xigdatas_date_ts,

           /* week end date */
          CASE WHEN EXTRACT(DAYOFWEEK FROM date) = 1
          THEN TIMESTAMP_ADD(date , INTERVAL 5 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 2
          THEN TIMESTAMP_ADD(date , INTERVAL 4 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 3
          THEN TIMESTAMP_ADD(date, INTERVAL 3 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 4
          THEN TIMESTAMP_ADD(date, INTERVAL 2 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 5
          THEN TIMESTAMP_ADD(date, INTERVAL 1 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 6
          THEN date

          WHEN EXTRACT(DAYOFWEEK FROM date ) = 7
          THEN TIMESTAMP_ADD(date, INTERVAL 6 DAY)

          ELSE NULL END as week_end_date_ts,


          /* Difference between current date and week end date */
          DATE_DIFF(

          CASE WHEN EXTRACT(DAYOFWEEK FROM date) = 1
          THEN TIMESTAMP_ADD(date , INTERVAL 5 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 2
          THEN TIMESTAMP_ADD(date , INTERVAL 4 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 3
          THEN TIMESTAMP_ADD(date, INTERVAL 3 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 4
          THEN TIMESTAMP_ADD(date, INTERVAL 2 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 5
          THEN TIMESTAMP_ADD(date, INTERVAL 1 DAY)

          WHEN EXTRACT(DAYOFWEEK FROM date) = 6
          THEN date

          WHEN EXTRACT(DAYOFWEEK FROM date ) = 7
          THEN TIMESTAMP_ADD(date, INTERVAL 6 DAY)

          ELSE NULL END,

          date,
          DAY) +  CASE WHEN period = 'Morning' THEN 0.2
               WHEN period = 'Evening' THEN 0.1
               ELSE 0.2 END as week_end_date_offset,


           /* End of week date */
           MAX(value) as xigdatas_value_PT,

           /*
           MAX(CASE WHEN symbol = 'AG' THEN value ELSE NULL END) as xigdatas_value_AG
           MAX(CASE WHEN symbol = 'AU' THEN value ELSE NULL END) as xigdatas_value_AU,
           MAX(CASE WHEN symbol = 'PD' THEN value ELSE NULL END) as xigdatas_value_PD,
           MAX(CASE WHEN symbol = 'PT' THEN value ELSE NULL END) as xigdatas_value_PT
           */

           FROM `dummy.dummy.xigdatas`

           WHERE currency = 'USD' AND symbol = 'PT' AND value IS NOT NULL

                 AND date BETWEEN (SELECT start_date_ts FROM TEMP_dateSelector)

             AND (SELECT end_date_ts FROM TEMP_dateSelector)


           GROUP BY xigdatas_date_ts, week_end_date_ts, week_end_date_offset),
           
           
   xigdatas_AG_transformed_by_week_end as (
           
         SELECT xigdatas_AG_tb.xigdatas_date_ts, 
                xigdatas_AG_tb.week_end_date_ts,
                MAX(xigdatas_AG_tb.xigdatas_value_AG) as xigdatas_value_AG

         FROM xigdatas_AG as xigdatas_AG_tb

         LEFT JOIN (SELECT week_end_date_ts,
                           MIN(week_end_date_offset) as week_end_date_offset_min

                    FROM xigdatas_AG
                    GROUP BY week_end_date_ts) as xigdatas_AG_tb_min

        ON xigdatas_AG_tb.week_end_date_ts = xigdatas_AG_tb_min.week_end_date_ts

        WHERE xigdatas_AG_tb.week_end_date_offset = xigdatas_AG_tb_min.week_end_date_offset_min
        
        GROUP BY xigdatas_AG_tb.xigdatas_date_ts,
                 xigdatas_AG_tb.week_end_date_ts
        
        ),
        
        
  xigdatas_AU_transformed_by_week_end as (
           
         SELECT xigdatas_AU_tb.xigdatas_date_ts, 
                xigdatas_AU_tb.week_end_date_ts,
                MAX(xigdatas_AU_tb.xigdatas_value_AU) as xigdatas_value_AU

         FROM xigdatas_AU as xigdatas_AU_tb

         LEFT JOIN (SELECT week_end_date_ts,
                           MIN(week_end_date_offset) as week_end_date_offset_min

                    FROM xigdatas_AU
                    GROUP BY week_end_date_ts) as xigdatas_AU_tb_min

        ON xigdatas_AU_tb.week_end_date_ts = xigdatas_AU_tb_min.week_end_date_ts

        WHERE xigdatas_AU_tb.week_end_date_offset = xigdatas_AU_tb_min.week_end_date_offset_min
        
        GROUP BY xigdatas_AU_tb.xigdatas_date_ts, 
                xigdatas_AU_tb.week_end_date_ts
        
        ),
        
        
   xigdatas_PD_transformed_by_week_end as (
           
         SELECT xigdatas_PD_tb.xigdatas_date_ts, 
                xigdatas_PD_tb.week_end_date_ts,
                MAX(xigdatas_PD_tb.xigdatas_value_PD) as xigdatas_value_PD

         FROM xigdatas_PD as xigdatas_PD_tb

         LEFT JOIN (SELECT week_end_date_ts,
                           MIN(week_end_date_offset) as week_end_date_offset_min

                    FROM xigdatas_PD
                    GROUP BY week_end_date_ts) as xigdatas_PD_tb_min

        ON xigdatas_PD_tb.week_end_date_ts = xigdatas_PD_tb_min.week_end_date_ts

        WHERE xigdatas_PD_tb.week_end_date_offset = xigdatas_PD_tb_min.week_end_date_offset_min
        
        GROUP BY xigdatas_PD_tb.xigdatas_date_ts, 
                 xigdatas_PD_tb.week_end_date_ts
        
        ),
        
        
        
    xigdatas_PT_transformed_by_week_end as (
           
         SELECT xigdatas_PT_tb.xigdatas_date_ts, 
                xigdatas_PT_tb.week_end_date_ts,
                MAX(xigdatas_PT_tb.xigdatas_value_PT) as xigdatas_value_PT

         FROM xigdatas_PT as xigdatas_PT_tb

         LEFT JOIN (SELECT week_end_date_ts,
                           MIN(week_end_date_offset) as week_end_date_offset_min

                    FROM xigdatas_PT
                    GROUP BY week_end_date_ts) as xigdatas_PT_tb_min

        ON xigdatas_PT_tb.week_end_date_ts = xigdatas_PT_tb_min.week_end_date_ts

        WHERE xigdatas_PT_tb.week_end_date_offset = xigdatas_PT_tb_min.week_end_date_offset_min
        
        GROUP BY xigdatas_PT_tb.xigdatas_date_ts, 
                 xigdatas_PT_tb.week_end_date_ts
        
        ),



     xigdatas_AU_transformed_by_month_end as

       (SELECT  week_end_date_ts,
               EXTRACT(Month from week_end_date_ts) as week_end_date_month_num,

               CASE WHEN EXTRACT(Month from week_end_date_ts) = EXTRACT(Month from TIMESTAMP_ADD(week_end_date_ts, INTERVAL 7 DAY)) THEN False
                    ELSE True END as last_month_week,
        
               /* Aggregation is not needed, but just as a good practive */
               MAX(xigdatas_value_AU) as xigdatas_value_AU,

 
 
          FROM xigdatas_AU_transformed_by_week_end
          GROUP BY week_end_date_ts, week_end_date_month_num, last_month_week
          HAVING last_month_week = True),



      xigdatas_AG_transformed_by_month_end as

       (SELECT  week_end_date_ts,
               EXTRACT(Month from week_end_date_ts) as week_end_date_month_num,

               CASE WHEN EXTRACT(Month from week_end_date_ts) = EXTRACT(Month from TIMESTAMP_ADD(week_end_date_ts, INTERVAL 7 DAY)) THEN False
                    ELSE True END as last_month_week,
        
               /* Aggregation is not needed, but just as a good practive */
               MAX(xigdatas_value_AG) as xigdatas_value_AG,

 
 
          FROM xigdatas_AG_transformed_by_week_end 
          GROUP BY week_end_date_ts, week_end_date_month_num, last_month_week
          HAVING last_month_week = True),


     xigdatas_PD_transformed_by_month_end as

       (SELECT  week_end_date_ts,
               EXTRACT(Month from week_end_date_ts) as week_end_date_month_num,

               CASE WHEN EXTRACT(Month from week_end_date_ts) = EXTRACT(Month from TIMESTAMP_ADD(week_end_date_ts, INTERVAL 7 DAY)) THEN False
                    ELSE True END as last_month_week,
        
               /* Aggregation is not needed, but just as a good practive */
               MAX(xigdatas_value_PD) as xigdatas_value_PD,

 
 
          FROM xigdatas_PD_transformed_by_week_end 
          GROUP BY week_end_date_ts, week_end_date_month_num, last_month_week
          HAVING last_month_week = True),


     xigdatas_PT_transformed_by_month_end as

       (SELECT  week_end_date_ts,
               EXTRACT(Month from week_end_date_ts) as week_end_date_month_num,

               CASE WHEN EXTRACT(Month from week_end_date_ts) = EXTRACT(Month from TIMESTAMP_ADD(week_end_date_ts, INTERVAL 7 DAY)) THEN False
                    ELSE True END as last_month_week,
        
               /* Aggregation is not needed, but just as a good practive */
               MAX(xigdatas_value_PT) as xigdatas_value_PT,

 
 
          FROM xigdatas_PT_transformed_by_week_end 
          GROUP BY week_end_date_ts, week_end_date_month_num, last_month_week
          HAVING last_month_week = True)


  

     
 
      

     SELECT offersAuditConsolidated_tb.*,
          CONCAT(CAST(offersAuditConsolidated_tb.week_start_date as Date), " - ", CAST(offersAuditConsolidated_tb.week_end_date as Date)) as week_start_end,
          /* EXTRACT(MONTH FROM offersAuditConsolidated_tb.week_end_date) as month_num_audit, */
          
          reps_tb.name as reps_name,
          CASE WHEN history_territory_tb.name IS NULL THEN "-" ELSE history_territory_tb.name END as territory_name,


          /* Audit Grams calculations */
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_ag_percents / 100) as audit_wc_grams_AG,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_au_percents / 100) as audit_wc_grams_AU,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_pd_percents / 100) as audit_wc_grams_PD,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_pt_percents / 100) as audit_wc_grams_PT,

          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_ag_percents / 100) as audit_wd_grams_AG,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_au_percents / 100) as audit_wd_grams_AU,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_pd_percents / 100) as audit_wd_grams_PD,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_pt_percents / 100) as audit_wd_grams_PT,

          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_ag_percents / 100) as audit_yc_grams_AG,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_au_percents / 100) as audit_yc_grams_AU,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_pd_percents / 100) as audit_yc_grams_PD,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_pt_percents / 100) as audit_yc_grams_PT,

          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_ag_percents / 100) as audit_yd_grams_AG,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_au_percents / 100) as audit_yd_grams_AU,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_pd_percents / 100) as audit_yd_grams_PD,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_pt_percents / 100) as audit_yd_grams_PT,

          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_ag_percents / 100) as audit_yr_grams_AG,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_au_percents / 100) as audit_yr_grams_AU,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_pd_percents / 100) as audit_yr_grams_PD,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_pt_percents / 100) as audit_yr_grams_PT,


          /* Prescious metals calculation (Using Weekly Price) */
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_ag_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AG_transformed_by_week_end_tb.xigdatas_value_AG AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_AG_weekly,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_au_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AU_transformed_by_week_end_tb.xigdatas_value_AU AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_AU_weekly,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_pd_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PD_transformed_by_week_end_tb.xigdatas_value_PD AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_PD_weekly,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_pt_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PT_transformed_by_week_end_tb.xigdatas_value_PT AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_PT_weekly,

          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_ag_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AG_transformed_by_week_end_tb.xigdatas_value_AG AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_AG_weekly,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_au_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AU_transformed_by_week_end_tb.xigdatas_value_AU AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_AU_weekly,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_pd_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PD_transformed_by_week_end_tb.xigdatas_value_PD AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_PD_weekly,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_pt_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PT_transformed_by_week_end_tb.xigdatas_value_PT AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_PT_weekly,

          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_ag_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AG_transformed_by_week_end_tb.xigdatas_value_AG AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_AG_weekly,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_au_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AU_transformed_by_week_end_tb.xigdatas_value_AU AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_AU_weekly,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_pd_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PD_transformed_by_week_end_tb.xigdatas_value_PD AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_PD_weekly,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_pt_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PT_transformed_by_week_end_tb.xigdatas_value_PT AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_PT_weekly,

          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_ag_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AG_transformed_by_week_end_tb.xigdatas_value_AG AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_AG_weekly,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_au_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AU_transformed_by_week_end_tb.xigdatas_value_AU AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_AU_weekly,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_pd_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PD_transformed_by_week_end_tb.xigdatas_value_PD AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_PD_weekly,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_pt_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PT_transformed_by_week_end_tb.xigdatas_value_PT AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_PT_weekly,

          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_ag_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AG_transformed_by_week_end_tb.xigdatas_value_AG AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_AG_weekly,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_au_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AU_transformed_by_week_end_tb.xigdatas_value_AU AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_AU_weekly,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_pd_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PD_transformed_by_week_end_tb.xigdatas_value_PD AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_PD_weekly,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_pt_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PT_transformed_by_week_end_tb.xigdatas_value_PT AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_PT_weekly,

          /* Prescious metals calculation (Using End of Month Price) */
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_ag_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AG_transformed_by_month_end_tb.xigdatas_value_AG AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_AG_month_fix,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_au_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AU_transformed_by_month_end_tb.xigdatas_value_AU AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_AU_month_fix,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_pd_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PD_transformed_by_month_end_tb.xigdatas_value_PD AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_PD_month_fix,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_pt_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PT_transformed_by_month_end_tb.xigdatas_value_PT AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_PT_month_fix,

          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_ag_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AG_transformed_by_month_end_tb.xigdatas_value_AG AS BigDecimal), CAST(31.1035 AS Bigdecimal)) as audit_wd_est_mkt_value_AG_month_fix,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_au_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AU_transformed_by_month_end_tb.xigdatas_value_AU AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_AU_month_fix,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_pd_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PD_transformed_by_month_end_tb.xigdatas_value_PD AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_PD_month_fix,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_pt_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PT_transformed_by_month_end_tb.xigdatas_value_PT AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_PT_month_fix,

          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_ag_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AG_transformed_by_month_end_tb.xigdatas_value_AG AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_AG_month_fix,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_au_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AU_transformed_by_month_end_tb.xigdatas_value_AU AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_AU_month_fix,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_pd_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PD_transformed_by_month_end_tb.xigdatas_value_PD AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_PD_month_fix,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_pt_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PT_transformed_by_month_end_tb.xigdatas_value_PT AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_PT_month_fix,

          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_ag_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AG_transformed_by_month_end_tb.xigdatas_value_AG AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_AG_month_fix,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_au_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AU_transformed_by_month_end_tb.xigdatas_value_AU AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_AU_month_fix,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_pd_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PD_transformed_by_month_end_tb.xigdatas_value_PD AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_PD_month_fix,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_pt_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PT_transformed_by_month_end_tb.xigdatas_value_PT AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_PT_month_fix,

          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_ag_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AG_transformed_by_month_end_tb.xigdatas_value_AG AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_AG_month_fix,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_au_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_AU_transformed_by_month_end_tb.xigdatas_value_AU AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_AU_month_fix,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_pd_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PD_transformed_by_month_end_tb.xigdatas_value_PD AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_PD_month_fix,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_pt_percents / 100) * dummy.bigDecimal_division(CAST(xigdatas_PT_transformed_by_month_end_tb.xigdatas_value_PT AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_PT_month_fix,

          
          /* Prescious metals calculation (Settlement Price) */
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_ag_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.AG_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_AG_settlement,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_au_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.AU_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_AU_settlement,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_pd_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.PD_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_PD_settlement,
          ((offersAuditConsolidated_tb.wc_a + offersAuditConsolidated_tb.wc_ar) * history_alloy_tables_tb.wc_pt_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.PT_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wc_est_mkt_value_PT_settlement,

          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_ag_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.AG_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_AG_settlement,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_au_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.AU_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_AU_settlement,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_pd_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.PD_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_PD_settlement,
          ((offersAuditConsolidated_tb.wd_a + offersAuditConsolidated_tb.wd_ar) * history_alloy_tables_tb.wd_pt_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.PT_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_wd_est_mkt_value_PT_settlement,

          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_ag_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.AG_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_AG_settlement,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_au_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.AU_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_AU_settlement,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_pd_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.PD_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_PD_settlement,
          ((offersAuditConsolidated_tb.yc_a + offersAuditConsolidated_tb.yc_ar) * history_alloy_tables_tb.yc_pt_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.PT_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yc_est_mkt_value_PT_settlement,

          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_ag_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.AG_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_AG_settlement,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_au_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.AU_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_AU_settlement,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_pd_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.PD_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_PD_settlement,
          ((offersAuditConsolidated_tb.yd_a + offersAuditConsolidated_tb.yd_ar) * history_alloy_tables_tb.yd_pt_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.PT_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yd_est_mkt_value_PT_settlement,

          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_ag_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.AG_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_AG_settlement,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_au_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.AU_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_AU_settlement,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_pd_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.PD_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_PD_settlement,
          ((offersAuditConsolidated_tb.yr_a + offersAuditConsolidated_tb.yr_ar) * history_alloy_tables_tb.yr_pt_percents / 100) * dummy.bigDecimal_division(CAST(settlement_price_tb.PT_value AS BigDecimal), CAST(31.1035 AS BigDecimal)) as audit_yr_est_mkt_value_PT_settlement,


          plan_quota_yearly_tb.plan_quota

          / CASE WHEN offersAuditConsolidated_tb.weekCount IS NULL
                 OR offersAuditConsolidated_tb.weekCount = 0
                 THEN 1 ELSE offersAuditConsolidated_tb.weekCount END

          / CASE WHEN offers_territory_id_count IS NULL
            OR offers_territory_id_count = 0
          THEN 1 ELSE offers_territory_id_count END
          
          AS plan_quota_yearly,


          
          /* Settlement Prices */
          settlement_price_tb.AG_value as settlement_price_AG_value,
          settlement_price_tb.PD_value as settlement_price_PD_value,
          settlement_price_tb.PT_value as settlement_price_PT_value,
          settlement_price_tb.AU_value as settlement_price_AU_value,
          CASE WHEN settlement_price_tb.date_ts != "" AND settlement_price_tb.date_ts IS NOT NULL
               THEN PARSE_TIMESTAMP("%Y-%m-%d", settlement_price_tb.date_ts)
               ELSE NULL
               END AS settlement_price__date,

          /* Base Pay */
          reps_base_pay_tb.base_pay
          / CASE WHEN offersAuditConsolidated_tb.weekCount IS NULL
                 OR offersAuditConsolidated_tb.weekCount = 0
                 THEN 1 ELSE offersAuditConsolidated_tb.weekCount
            END
          
           / CASE WHEN offers_territory_id_count IS NULL
            OR offers_territory_id_count = 0
           THEN 1 ELSE offers_territory_id_count END
            
            as reps_base_pay_value,

          /* history_reps_bcr_tb */
          history_reps_bcr_tb.bcr_value as reps_bcr_value,

          /* history alloy tables percents */
          history_alloy_tables_tb.yc_ag_percents AS alloy_tables_yc_ag_percents,
          history_alloy_tables_tb.yc_au_percents AS alloy_tables_yc_au_percents,
          history_alloy_tables_tb.yc_pd_percents AS alloy_tables_yc_pd_percents,
          history_alloy_tables_tb.yc_pt_percents AS alloy_tables_yc_pt_percents,

          history_alloy_tables_tb.yd_ag_percents AS alloy_tables_yd_ag_percents,
          history_alloy_tables_tb.yd_au_percents AS alloy_tables_yd_au_percents,
          history_alloy_tables_tb.yd_pd_percents AS alloy_tables_yd_pd_percents,
          history_alloy_tables_tb.yd_pt_percents AS alloy_tables_yd_pt_percents,

          history_alloy_tables_tb.yr_ag_percents AS alloy_tables_yr_ag_percents,
          history_alloy_tables_tb.yr_au_percents AS alloy_tables_yr_au_percents,
          history_alloy_tables_tb.yr_pd_percents AS alloy_tables_yr_pd_percents,
          history_alloy_tables_tb.yr_pt_percents AS alloy_tables_yr_pt_percents,

          history_alloy_tables_tb.wc_ag_percents AS alloy_tables_wc_ag_percents,
          history_alloy_tables_tb.wc_au_percents AS alloy_tables_wc_au_percents,
          history_alloy_tables_tb.wc_pd_percents AS alloy_tables_wc_pd_percents,
          history_alloy_tables_tb.wc_pt_percents AS alloy_tables_wc_pt_percents,

          history_alloy_tables_tb.wd_ag_percents AS alloy_tables_wd_ag_percents,
          history_alloy_tables_tb.wd_au_percents AS alloy_tables_wd_au_percents,
          history_alloy_tables_tb.wd_pd_percents AS alloy_tables_wd_pd_percents,
          history_alloy_tables_tb.wd_pt_percents AS alloy_tables_wd_pt_percents,


          /* Melts and refinery data */
          CASE WHEN melts_tb.melt_ids IS NULL OR melts_tb.melt_ids = "" THEN "-"
               ELSE melts_tb.melt_ids
               END as melt_ids,

          CASE WHEN melts_tb.melt_potnumber IS NULL OR melts_tb.melt_potnumber = "" THEN "-"
               ELSE CAST(melts_tb.melt_potnumber AS String)
               END as melt_potnumber,

          melts_tb.melt_refinery AS melt_refinery,

          melts_tb.melt_assay_pd_market_value as melt_assay_pd_market_value,
          melts_tb.melt_assay_pt_market_value as melt_assay_pt_market_value,
          melts_tb.melt_assay_au_market_value as melt_assay_au_market_value,
          melts_tb.melt_assay_ag_market_value as melt_assay_ag_market_value,

          /* (settlement_price_tb.AG_value / 31.1035) */
          /*
          melts_tb.rafinery_assay_ag_grams * (settlement_price_tb.AG_value / 31.1035) as refinery_est_mkt_value_AG_settlement,
          melts_tb.rafinery_assay_au_grams * (settlement_price_tb.AU_value / 31.1035) as refinery_est_mkt_value_AU_settlement,
          melts_tb.rafinery_assay_pd_grams * (settlement_price_tb.PD_value / 31.1035) as refinery_est_mkt_value_PD_settlement,
          melts_tb.rafinery_assay_pt_grams * (settlement_price_tb.PT_value / 31.1035) as refinery_est_mkt_value_PT_settlement,
          */

          melts_tb.rafinery_assay_ag_grams as rafinery_assay_ag_grams,
          melts_tb.rafinery_assay_au_grams as rafinery_assay_au_grams,
          melts_tb.rafinery_assay_pd_grams as rafinery_assay_pd_grams,
          melts_tb.rafinery_assay_pt_grams as rafinery_assay_pt_grams,

          melts_tb.rafinery_assay_ag_dollar_per_gram_avg as rafinery_assay_ag_dollar_per_gram_avg,
          melts_tb.rafinery_assay_au_dollar_per_gram_avg as rafinery_assay_au_dollar_per_gram_avg,
          melts_tb.rafinery_assay_pd_dollar_per_gram_avg as rafinery_assay_pd_dollar_per_gram_avg,
          meltS_tb.rafinery_assay_pt_dollar_per_gram_avg as rafinery_assay_pt_dollar_per_gram_avg,

          melts_tb.rafinery_assay_pt_dollars as rafinery_assay_pt_dollars,
          melts_tb.rafinery_assay_pd_dollars as rafinery_assay_pd_dollars,
          melts_tb.rafinery_assay_au_dollars as rafinery_assay_au_dollars,
          melts_tb.rafinery_assay_ag_dollars as rafinery_assay_ag_dollars,

          /*
          melts_tb.rafinery_assay_AG_dollar_per_gram_without_terms_avg as rafinery_assay_AG_dollar_per_gram_without_terms_avg,
          melts_tb.rafinery_assay_AU_dollar_per_gram_without_terms_avg as rafinery_assay_AU_dollar_per_gram_without_terms_avg,
          melts_tb.rafinery_assay_PD_dollar_per_gram_without_terms_avg as rafinery_assay_PD_dollar_per_gram_without_terms_avg,
          melts_tb.rafinery_assay_PT_dollar_per_gram_without_terms_avg as rafinery_assay_PT_dollar_per_gram_without_terms_avg,
          */


          melts_tb.rafinery_assay_pt_dollars_without_terms as rafinery_assay_pt_dollars_without_terms,
          melts_tb.rafinery_assay_pd_dollars_without_terms as rafinery_assay_pd_dollars_without_terms,
          melts_tb.rafinery_assay_au_dollars_without_terms as rafinery_assay_au_dollars_without_terms,
          melts_tb.rafinery_assay_ag_dollars_without_terms as rafinery_assay_ag_dollars_without_terms,


          melts_tb.rafinery_assay_fee as rafinery_assay_fee,
          melts_tb.rafinery_market_value_at_fix as rafinery_market_value_at_fix,

          melts_tb.melt_assaypdweight as melt_assaypdweight,
          melts_tb.melt_assayptweight as melt_assayptweight,
          melts_tb.melt_assayauweight as melt_assayauweight,
          melts_tb.melt_assayagweight as melt_assayagweight

          /* melts_tb.rafinery_market_value_at_fix * history_reps_bcr_tb.bcr_value as rafinery_assay_earnings */

          

          /*
          CASE WHEN melts_tb.melt_assaypdweight IS NULL THEN 0 ELSE melts_tb.melt_assaypdweight END as melt_assaypdweight,
          CASE WHEN melts_tb.melt_assayptweight IS NULL THEN 0 ELSE melts_tb.melt_assayptweight END as melt_assayptweight,
          CASE WHEN melts_tb.melt_measuredweightwithbags IS NULL THEN 0 ELSE melts_tb.melt_measuredweightwithbags END as melt_measuredweightwithbags,
          CASE WHEN melts_tb.melt_assayweightloss IS NULL THEN 0 ELSE melts_tb.melt_assayweightloss END as melt_assayweightloss,
          CASE WHEN melts_tb.melt_achievedagweight IS NULL THEN 0 ELSE melts_tb.melt_achievedagweight END as melt_achievedagweight,
          CASE WHEN melts_tb.melt_meltfees IS NULL THEN 0 ELSE melts_tb.melt_meltfees END as melt_meltfees,
          CASE WHEN melts_tb.melt_achievedptvalue IS NULL THEN 0 ELSE melts_tb.melt_achievedptvalue END as melt_achievedptvalue,
          CASE WHEN melts_tb.melt_assayagvalue IS NULL THEN 0 ELSE melts_tb.melt_assayagvalue END as melt_assayagvalue,
          CASE WHEN melts_tb.melt_assaypdvalue IS NULL THEN 0 ELSE melts_tb.melt_assaypdvalue END as melt_assaypdvalue,
          CASE WHEN melts_tb.melt_assayptvalue IS NULL THEN 0 ELSE melts_tb.melt_assayptvalue END as melt_assayptvalue,
          CASE WHEN melts_tb.melt_achievednonpreciousweight IS NULL THEN 0 ELSE melts_tb.melt_achievednonpreciousweight END as melt_achievednonpreciousweight,
          CASE WHEN melts_tb.melt_achievedauweight IS NULL THEN 0 ELSE melts_tb.melt_achievedauweight END as melt_achievedauweight,
          CASE WHEN melts_tb.melt_achievedptweight IS NULL THEN 0 ELSE melts_tb.melt_achievedptweight END as melt_achievedptweight,
          CASE WHEN melts_tb.melt_assayfees IS NULL THEN 0 ELSE melts_tb.melt_assayfees END as melt_assayfees,
          CASE WHEN melts_tb.melt_assayauweight IS NULL THEN 0 ELSE melts_tb.melt_assayauweight END as melt_assayauweight,
          CASE WHEN melts_tb.melt_achievedpdweight IS NULL THEN 0 ELSE melts_tb.melt_achievedpdweight END as melt_achievedpdweight,
          CASE WHEN melts_tb.melt_achievedagvalue IS NULL THEN 0 ELSE melts_tb.melt_achievedagvalue END as melt_achievedagvalue,
          CASE WHEN melts_tb.melt_achievedpdvalue IS NULL THEN 0 ELSE melts_tb.melt_achievedpdvalue END as melt_achievedpdvalue,
          CASE WHEN melts_tb.melt_assayauvalue IS NULL THEN 0 ELSE melts_tb.melt_assayauvalue END as melt_assayauvalue,
          cASE WHEN melts_tb.melt_measuredbagsweight IS NULL THEN 0 ELSE melts_tb.melt_measuredbagsweight END as melt_measuredbagsweight,

          CASE WHEN melts_tb.melt_assayagweight IS NULL THEN 0 ELSE melts_tb.melt_assayagweight END as melt_assayagweight,
          CASE WHEN melts_tb.melt_achievedauvalue IS NULL THEN 0 ELSE melts_tb.melt_achievedauvalue END as melt_achievedauvalue,
          CASE WHEN melts_tb.melt_shippingfees IS NULL THEN 0 ELSE melts_tb.melt_shippingfees END as melt_shippingfees */





      
      FROM
      
      (SELECT 

         /* Date Dimensions */
         CASE WHEN dateDimension_tb.week_start_ts IS NOT NULL THEN dateDimension_tb.week_start_ts
              WHEN offersAudit_tb.offer_date_start_week IS NOT NULL THEN offersAudit_tb.offer_date_start_week
              WHEN plan_quota_tb.week_start_ts IS NOT NULL THEN plan_quota_tb.week_start_ts
              ELSE NULL
         END AS week_start_date,

         CASE WHEN dateDimension_tb.week_end_ts IS NOT NULL THEN dateDimension_tb.week_end_ts
              WHEN offersAudit_tb.offer_date_end_week IS NOT NULL THEN offersAudit_tb.offer_date_end_week
              WHEN plan_quota_tb.week_end_ts IS NOT NULL THEN plan_quota_tb.week_end_ts
              ELSE NULL
         END AS week_end_date,

         dateDimension_tb.year_num AS year_num,

         dateDimension_tb.month_num AS month_num,

         dateDimension_tb.weekCount AS weekCount,

         offersAudit_tb.offers_territory_id_count AS offers_territory_id_count,

         CAST(      
         CASE WHEN repsDimension_tb.rep_id IS NOT NULL THEN repsDimension_tb.rep_id
              WHEN offersAudit_tb.offers_rep_id IS NOT NULL THEN offersAudit_tb.offers_rep_id
              WHEN plan_quota_tb.rep_id IS NOT NULL THEN plan_quota_tb.rep_id
              ELSE NULL
          END
          AS int64) AS rep_id,
         
         offersAudit_tb.offers_territory_id AS territory_id,

         offersAudit_tb.movements,
         
         
          /* Offer Metrics */
          CASE WHEN offersAudit_tb.wc_grams IS NULL THEN 0 ELSE offersAudit_tb.wc_grams END as wc_grams,
          CASE WHEN offersAudit_tb.wd_grams IS NULL THEN 0 ELSE offersAudit_tb.wd_grams END as wd_grams,
          CASE WHEN offersAudit_tb.yc_grams IS NULL THEN 0 ELSE offersAudit_tb.yc_grams END as yc_grams,
          CASE WHEN offersAudit_tb.yd_grams IS NULL THEN 0 ELSE offersAudit_tb.yd_grams END as yd_grams,
          CASE WHEN offersAudit_tb.yr_grams IS NULL THEN 0 ELSE offersAudit_tb.yr_grams END as yr_grams,

          CASE WHEN offersAudit_tb.offers_total_offer IS NULL THEN 0 ELSE offersAudit_tb.offers_total_offer END as offers_total_offer,
          CASE WHEN offersAudit_tb.offers_total_offer_melt IS NULL THEN 0 ELSE offersAudit_tb.offers_total_offer_melt END as offers_total_offer_melt,
          CASE WHEN offersAudit_tb.offers_total_offer_assayed IS NULL THEN 0 ELSE offersAudit_tb.offers_total_offer_assayed END as offers_total_offer_assayed,
          CASE WHEN offersAudit_tb.offers_total_offer_melt_assayed IS NULL THEN 0 ELSE offersAudit_tb.offers_total_offer_melt_assayed END as offers_total_offer_melt_assayed,

          /* Audit Metrics */
          CASE WHEN offersAudit_tb.yc_a IS NULL THEN 0 ELSE offersAudit_tb.yc_a END as yc_a,
          CASE WHEN offersAudit_tb.yd_a IS NULL THEN 0 ELSE offersAudit_tb.yd_a END as yd_a,
          CASE WHEN offersAudit_tb.yr_a IS NULL THEN 0 ELSE offersAudit_tb.yr_a END as yr_a,
          CASE WHEN offersAudit_tb.wc_ar IS NULL THEN 0 ELSE offersAudit_tb.wc_ar END as wc_ar,
          CASE WHEN offersAudit_tb.wd_ar IS NULL THEN 0 ELSE offersAudit_tb.wd_ar END as wd_ar,
          CASE WHEN offersAudit_tb.wc_a IS NULL THEN 0 ELSE offersAudit_tb.wc_a END as wc_a,
          CASE WHEN offersAudit_tb.wd_a IS NULL THEN 0 ELSE offersAudit_tb.wd_a END as wd_a,
          CASE WHEN offersAudit_tb.yc_ar IS NULL THEN 0 ELSE offersAudit_tb.yc_ar END as yc_ar,
          CASE WHEN offersAudit_tb.yd_ar IS NULL THEN 0 ELSE offersAudit_tb.yd_ar END as yd_ar,
          CASE WHEN offersAudit_tb.yr_ar IS NULL THEN 0 ELSE offersAudit_tb.yr_ar END as yr_ar,
          CASE WHEN offersAudit_tb.nps_m IS NULL THEN 0 ELSE offersAudit_tb.nps_m END as nps_m,
          CASE WHEN offersAudit_tb.nps_o IS NULL THEN 0 ELSE offersAudit_tb.nps_o END as nps_o,

          /* Weekly Plan Metrics */
          CASE WHEN plan_quota_tb.plan_quota IS NULL THEN 0
               ELSE plan_quota_tb.plan_quota END

          / CASE WHEN offersAudit_tb.offers_territory_id_count IS NULL
            OR offersAudit_tb.offers_territory_id_count = 0
          THEN 1 ELSE offersAudit_tb.offers_territory_id_count
          END AS plan_quota_weekly
         
      
      FROM dateDimensionEnriched as dateDimension_tb
      CROSS JOIN repsDimension as repsDimension_tb

      LEFT JOIN TEMP_offersAuditEnriched as offersAudit_tb
      ON dateDimension_tb.week_start_ts = offersAudit_tb.offer_date_start_week
         AND dateDimension_tb.week_end_ts = offersAudit_tb.offer_date_end_week
         AND repsDimension_tb.rep_id = offersAudit_tb.offers_rep_id

      /* Join Transformed_reps_plan_quota data of weekly granuality */
      LEFT JOIN plan_quota as plan_quota_tb
      ON repsDimension_tb.rep_id = plan_quota_tb.rep_id
         AND dateDimension_tb.week_start_ts = plan_quota_tb.week_start_ts
         AND dateDimension_tb.week_end_ts = plan_quota_tb.week_end_ts) as offersAuditConsolidated_tb



     /* Join reps */
     LEFT JOIN `dummy.dummy.reps` as reps_tb
     ON offersAuditConsolidated_tb.rep_id = reps_tb.id


     /* Weekly Xigdatas */
     /* Join xigdatas weekly AG */
     LEFT JOIN xigdatas_AG_transformed_by_week_end as xigdatas_AG_transformed_by_week_end_tb
     ON offersAuditConsolidated_tb.week_end_date = xigdatas_AG_transformed_by_week_end_tb.week_end_date_ts

     /* Join xigdatas weekly AU */
     LEFT JOIN xigdatas_AU_transformed_by_week_end as xigdatas_AU_transformed_by_week_end_tb
     ON offersAuditConsolidated_tb.week_end_date = xigdatas_AU_transformed_by_week_end_tb.week_end_date_ts

     /* Join xigdatas weekly PD */
     LEFT JOIN xigdatas_PD_transformed_by_week_end as xigdatas_PD_transformed_by_week_end_tb
     ON offersAuditConsolidated_tb.week_end_date = xigdatas_PD_transformed_by_week_end_tb.week_end_date_ts

     /* Join xigdatas weekly PT */
     LEFT JOIN xigdatas_PT_transformed_by_week_end as xigdatas_PT_transformed_by_week_end_tb
     ON offersAuditConsolidated_tb.week_end_date = xigdatas_PT_transformed_by_week_end_tb.week_end_date_ts


     /* Monthly xigdatas */
     /* Join xigdatas monthly AG */
     LEFT JOIN xigdatas_AG_transformed_by_month_end as xigdatas_AG_transformed_by_month_end_tb
     ON offersAuditConsolidated_tb.month_num = xigdatas_AG_transformed_by_month_end_tb.week_end_date_month_num

     /* Join xigdatas monthly AU */
     LEFT JOIN xigdatas_AU_transformed_by_month_end as xigdatas_AU_transformed_by_month_end_tb
     ON offersAuditConsolidated_tb.month_num = xigdatas_AU_transformed_by_month_end_tb.week_end_date_month_num

     /* Join xigdatas monthly PD */
     LEFT JOIN xigdatas_PD_transformed_by_month_end as xigdatas_PD_transformed_by_month_end_tb
     ON offersAuditConsolidated_tb.month_num = xigdatas_PD_transformed_by_month_end_tb.week_end_date_month_num

     /* Join xigdatas monthly PT */
     LEFT JOIN xigdatas_PT_transformed_by_month_end as xigdatas_PT_transformed_by_month_end_tb
     ON offersAuditConsolidated_tb.month_num = xigdatas_PT_transformed_by_month_end_tb.week_end_date_month_num


     /* Join `dummy.dummy.reps_plan_quota` Yearly Plan  */
     LEFT JOIN plan_quota_yearly as plan_quota_yearly_tb
     ON offersAuditConsolidated_tb.rep_id  = plan_quota_yearly_tb.rep_id
        AND offersAuditConsolidated_tb.year_num = plan_quota_yearly_tb.year

     
     /* Join settlement_price data */
     LEFT JOIN `dummy.rep_dats_sheet.settlement_price` as settlement_price_tb
     ON offersAuditConsolidated_tb.year_num = settlement_price_tb.year
        AND offersAuditConsolidated_tb.month_num = settlement_price_tb.month


     /* Join reps_base_pay data */
     LEFT JOIN TEMP_base_pay as reps_base_pay_tb
     ON offersAuditConsolidated_tb.rep_id = reps_base_pay_tb.rep_id
        AND offersAuditConsolidated_tb.year_num = reps_base_pay_tb.year
        AND offersAuditConsolidated_tb.month_num = reps_base_pay_tb.month


     /* Join melt data. Joining outside of main table, since all rep_id, territory_id dimensions be aligned with offers in the main join */
     LEFT JOIN TEMP_meltsRefined as melts_tb
     ON offersAuditConsolidated_tb.rep_id = melts_tb.rep_id
        AND offersAuditConsolidated_tb.territory_id = melts_tb.territory_id
        AND offersAuditConsolidated_tb.week_start_date = melts_tb.week_start_date
        AND offersAuditConsolidated_tb.week_end_date = melts_tb.week_end_date


     /* Join SCD Territory */
     LEFT JOIN `dummy.dummy.history_territory` as history_territory_tb

     ON offersAuditConsolidated_tb.territory_id = history_territory_tb.id
        AND offersAuditConsolidated_tb.week_start_date >= history_territory_tb.start_ts
        AND (history_territory_tb.end_ts IS NULL OR offersAuditConsolidated_tb.week_end_date < history_territory_tb.end_ts)
     
     

     /* Join Territory
      LEFT JOIN `dummy.dummy.Territory` as history_territory_tb
      ON offersAuditConsolidated_tb.territory_id = history_territory_tb.id
      */



     /* Join SCD alloy tables */
     LEFT JOIN `dummy.dummy.history_alloy_tables` as history_alloy_tables_tb
     ON history_territory_tb.name = history_alloy_tables_tb.territory_name
        AND offersAuditConsolidated_tb.week_start_date >= history_alloy_tables_tb.start_ts
        AND (history_alloy_tables_tb.end_ts IS NULL OR offersAuditConsolidated_tb.week_end_date < history_alloy_tables_tb.end_ts)
     
     
     /*
     LEFT JOIN `dummy.dummy.view_alloy_tables` as history_alloy_tables_tb
     ON history_territory_tb.name = history_alloy_tables_tb.territory_name
     */

     /* Join SCD - BCR ratio information from tables - OLD OLD OLD
     LEFT JOIN `dummy.dummy.history_reps_bcr` as history_reps_bcr_tb
     ON offersAuditConsolidated_tb.rep_id = history_reps_bcr_tb.rep_id
        AND offersAuditConsolidated_tb.week_start_date >= history_reps_bcr_tb.start_ts
        AND (history_reps_bcr_tb.end_ts IS NULL OR offersAuditConsolidated_tb.week_end_date < history_reps_bcr_tb.end_ts);
     /*

     /* Join SCD - BCR ratio information from tables
     LEFT JOIN `dummy.rep_dats_sheet.reps_bcr` as history_reps_bcr_tb
     ON offersAuditConsolidated_tb.rep_id = history_reps_bcr_tb.rep_id
        AND offersAuditConsolidated_tb.year_num = history_reps_bcr_tb.year
      */

     LEFT JOIN TEMP_bcr_value as history_reps_bcr_tb
     ON offersAuditConsolidated_tb.rep_id = history_reps_bcr_tb.rep_id
        AND offersAuditConsolidated_tb.year_num = history_reps_bcr_tb.year
        AND offersAuditConsolidated_tb.month_num = history_reps_bcr_tb.month;
        

/* Persist TEMP tables for drill downreports */
/* TEMP_meltsAllocated -> TEMP_HISTORY_meltsAllocated */
IF (SELECT count(*) FROM dummy.__TABLES__ WHERE table_id='STAGING_spendLogAudit_meltsAllocated') = 0 THEN
CREATE TABLE IF NOT EXISTS `dummy.STAGING_spendLogAudit_meltsAllocated`
  AS
SELECT * FROM TEMP_meltsAllocated;
ELSE
     BEGIN
      /* DELETE OLD DATA */
     DELETE FROM `dummy.STAGING_spendLogAudit_meltsAllocated`
     WHERE week_start_date >= (SELECT min(week_start_date) FROM TEMP_meltsAllocated)
          AND week_end_date <= (SELECT max(week_end_date) FROM TEMP_meltsAllocated);

     /* Load new data into a table */
     INSERT INTO `dummy.STAGING_spendLogAudit_meltsAllocated`
     SELECT * FROM TEMP_meltsAllocated;
     END;
END IF;



/* TEMP_offersToMeltLinkEnriched -> STAGING_spendLogAudit_offersToMeltLinkEnriched */
IF (SELECT count(*) FROM dummy.__TABLES__ WHERE table_id='STAGING_spendLogAudit_offersToMeltLinkEnriched') = 0 THEN
CREATE TABLE IF NOT EXISTS `dummy.STAGING_spendLogAudit_offersToMeltLinkEnriched`
  AS
SELECT * FROM TEMP_offersToMeltLinkEnriched;
ELSE
     BEGIN
      /* DELETE OLD DATA */
     DELETE FROM `dummy.STAGING_spendLogAudit_offersToMeltLinkEnriched`
     WHERE week_start_date >= (SELECT min(week_start_date) FROM TEMP_offersToMeltLinkEnriched)
          AND week_end_date <= (SELECT max(week_end_date) FROM TEMP_offersToMeltLinkEnriched);

     /* Load new data into a table */
     INSERT INTO `dummy.STAGING_spendLogAudit_offersToMeltLinkEnriched`
     SELECT * FROM TEMP_offersToMeltLinkEnriched;
     END;
END IF;





/* Main report Loading */

IF (SELECT count(*) FROM dummy.__TABLES__ WHERE table_id='STAGING_spendLogAudit') = 0 THEN
  CREATE TABLE IF NOT EXISTS `dummy.STAGING_spendLogAudit`
  AS
  SELECT
    week_start_date,
    week_end_date,
    year_num,
    month_num,
    weekCount,
    offers_territory_id_count,
    rep_id,
    territory_id,
    wc_grams,
    wd_grams,
    yc_grams,
    yd_grams,
    yr_grams,
    offers_total_offer,
    offers_total_offer_melt,
    yc_a,
    yd_a,
    yr_a,
    wc_ar,
    wd_ar,
    wc_a,
    wd_a,
    yc_ar,
    yd_ar,
    yr_ar,
    nps_m,
    nps_o,
    plan_quota_weekly,
    week_start_end,
    reps_name,
    territory_name,
    audit_wc_grams_AG,
    audit_wc_grams_AU,
    audit_wc_grams_PD,
    audit_wc_grams_PT,
    audit_wd_grams_AG,
    audit_wd_grams_AU,
    audit_wd_grams_PD,
    audit_wd_grams_PT,
    audit_yc_grams_AG,
    audit_yc_grams_AU,
    audit_yc_grams_PD,
    audit_yc_grams_PT,
    audit_yd_grams_AG,
    audit_yd_grams_AU,
    audit_yd_grams_PD,
    audit_yd_grams_PT,
    audit_yr_grams_AG,
    audit_yr_grams_AU,
    audit_yr_grams_PD,
    audit_yr_grams_PT,
    audit_wc_est_mkt_value_AG_weekly,
    audit_wc_est_mkt_value_AU_weekly,
    audit_wc_est_mkt_value_PD_weekly,
    audit_wc_est_mkt_value_PT_weekly,
    audit_wd_est_mkt_value_AG_weekly,
    audit_wd_est_mkt_value_AU_weekly,
    audit_wd_est_mkt_value_PD_weekly,
    audit_wd_est_mkt_value_PT_weekly,
    audit_yc_est_mkt_value_AG_weekly,
    audit_yc_est_mkt_value_AU_weekly,
    audit_yc_est_mkt_value_PD_weekly,
    audit_yc_est_mkt_value_PT_weekly,
    audit_yd_est_mkt_value_AG_weekly,
    audit_yd_est_mkt_value_AU_weekly,
    audit_yd_est_mkt_value_PD_weekly,
    audit_yd_est_mkt_value_PT_weekly,
    audit_yr_est_mkt_value_AG_weekly,
    audit_yr_est_mkt_value_AU_weekly,
    audit_yr_est_mkt_value_PD_weekly,
    audit_yr_est_mkt_value_PT_weekly,
    audit_wc_est_mkt_value_AG_month_fix,
    audit_wc_est_mkt_value_AU_month_fix,
    audit_wc_est_mkt_value_PD_month_fix,
    audit_wc_est_mkt_value_PT_month_fix,
    audit_wd_est_mkt_value_AG_month_fix,
    audit_wd_est_mkt_value_AU_month_fix,
    audit_wd_est_mkt_value_PD_month_fix,
    audit_wd_est_mkt_value_PT_month_fix,
    audit_yc_est_mkt_value_AG_month_fix,
    audit_yc_est_mkt_value_AU_month_fix,
    audit_yc_est_mkt_value_PD_month_fix,
    audit_yc_est_mkt_value_PT_month_fix,
    audit_yd_est_mkt_value_AG_month_fix,
    audit_yd_est_mkt_value_AU_month_fix,
    audit_yd_est_mkt_value_PD_month_fix,
    audit_yd_est_mkt_value_PT_month_fix,
    audit_yr_est_mkt_value_AG_month_fix,
    audit_yr_est_mkt_value_AU_month_fix,
    audit_yr_est_mkt_value_PD_month_fix,
    audit_yr_est_mkt_value_PT_month_fix,
    audit_wc_est_mkt_value_AG_settlement,
    audit_wc_est_mkt_value_AU_settlement,
    audit_wc_est_mkt_value_PD_settlement,
    audit_wc_est_mkt_value_PT_settlement,
    audit_wd_est_mkt_value_AG_settlement,
    audit_wd_est_mkt_value_AU_settlement,
    audit_wd_est_mkt_value_PD_settlement,
    audit_wd_est_mkt_value_PT_settlement,
    audit_yc_est_mkt_value_AG_settlement,
    audit_yc_est_mkt_value_AU_settlement,
    audit_yc_est_mkt_value_PD_settlement,
    audit_yc_est_mkt_value_PT_settlement,
    audit_yd_est_mkt_value_AG_settlement,
    audit_yd_est_mkt_value_AU_settlement,
    audit_yd_est_mkt_value_PD_settlement,
    audit_yd_est_mkt_value_PT_settlement,
    audit_yr_est_mkt_value_AG_settlement,
    audit_yr_est_mkt_value_AU_settlement,
    audit_yr_est_mkt_value_PD_settlement,
    audit_yr_est_mkt_value_PT_settlement,
    plan_quota_yearly,
    settlement_price_AG_value,
    settlement_price_PD_value,
    settlement_price_PT_value,
    settlement_price_AU_value,
    settlement_price__date,
    reps_base_pay_value,
    reps_bcr_value,
    alloy_tables_yc_ag_percents,
    alloy_tables_yc_au_percents,
    alloy_tables_yc_pd_percents,
    alloy_tables_yc_pt_percents,
    alloy_tables_yd_ag_percents,
    alloy_tables_yd_au_percents,
    alloy_tables_yd_pd_percents,
    alloy_tables_yd_pt_percents,
    alloy_tables_yr_ag_percents,
    alloy_tables_yr_au_percents,
    alloy_tables_yr_pd_percents,
    alloy_tables_yr_pt_percents,
    alloy_tables_wc_ag_percents,
    alloy_tables_wc_au_percents,
    alloy_tables_wc_pd_percents,
    alloy_tables_wc_pt_percents,
    alloy_tables_wd_ag_percents,
    alloy_tables_wd_au_percents,
    alloy_tables_wd_pd_percents,
    alloy_tables_wd_pt_percents,
    melt_potnumber,
    melt_refinery,
    melt_assay_pd_market_value,
    melt_assay_pt_market_value,
    melt_assay_au_market_value,
    melt_assay_ag_market_value,
    rafinery_assay_ag_grams,
    rafinery_assay_au_grams,
    rafinery_assay_pd_grams,
    rafinery_assay_pt_grams,
    rafinery_assay_ag_dollar_per_gram_avg,
    rafinery_assay_au_dollar_per_gram_avg,
    rafinery_assay_pd_dollar_per_gram_avg,
    rafinery_assay_pt_dollar_per_gram_avg,
    rafinery_assay_pt_dollars,
    rafinery_assay_pd_dollars,
    rafinery_assay_au_dollars,
    rafinery_assay_ag_dollars,
    
    NULL as refinery_est_mkt_value_AG_settlement,
    NULL as refinery_est_mkt_value_AU_settlement,
    NULL as refinery_est_mkt_value_PD_settlement,
    NULL as refinery_est_mkt_value_PT_settlement,

    NULL as rafinery_assay_AG_dollar_per_gram_without_terms_avg,
    NULL as rafinery_assay_AU_dollar_per_gram_without_terms_avg,
    NULL as rafinery_assay_PD_dollar_per_gram_without_terms_avg,
    NULL as rafinery_assay_PT_dollar_per_gram_without_terms_avg,

    rafinery_assay_pt_dollars_without_terms,
    rafinery_assay_pd_dollars_without_terms,
    rafinery_assay_au_dollars_without_terms,
    rafinery_assay_ag_dollars_without_terms,
    
    rafinery_assay_fee,
    rafinery_market_value_at_fix,
    melt_ids,
    movements,
    offers_total_offer_assayed,
    offers_total_offer_melt_assayed,
    melt_assaypdweight,
    melt_assayptweight,
    melt_assayauweight,
    melt_assayagweight

  
  
  FROM LoadTable_REPORT_spendLogAudit;

ELSE
  /* DELETE OLD DATA */
  DELETE FROM `dummy.STAGING_spendLogAudit`
  WHERE week_start_date >= (SELECT min(week_start_date) FROM LoadTable_REPORT_spendLogAudit)
        AND week_end_date <= (SELECT max(week_end_date) FROM LoadTable_REPORT_spendLogAudit);

  /* Load new data into a table */
  INSERT INTO `dummy.STAGING_spendLogAudit`
  SELECT
  
      week_start_date,
    week_end_date,
    year_num,
    month_num,
    weekCount,
    offers_territory_id_count,
    rep_id,
    territory_id,
    wc_grams,
    wd_grams,
    yc_grams,
    yd_grams,
    yr_grams,
    offers_total_offer,
    offers_total_offer_melt,
    yc_a,
    yd_a,
    yr_a,
    wc_ar,
    wd_ar,
    wc_a,
    wd_a,
    yc_ar,
    yd_ar,
    yr_ar,
    nps_m,
    nps_o,
    plan_quota_weekly,
    week_start_end,
    reps_name,
    territory_name,
    audit_wc_grams_AG,
    audit_wc_grams_AU,
    audit_wc_grams_PD,
    audit_wc_grams_PT,
    audit_wd_grams_AG,
    audit_wd_grams_AU,
    audit_wd_grams_PD,
    audit_wd_grams_PT,
    audit_yc_grams_AG,
    audit_yc_grams_AU,
    audit_yc_grams_PD,
    audit_yc_grams_PT,
    audit_yd_grams_AG,
    audit_yd_grams_AU,
    audit_yd_grams_PD,
    audit_yd_grams_PT,
    audit_yr_grams_AG,
    audit_yr_grams_AU,
    audit_yr_grams_PD,
    audit_yr_grams_PT,
    audit_wc_est_mkt_value_AG_weekly,
    audit_wc_est_mkt_value_AU_weekly,
    audit_wc_est_mkt_value_PD_weekly,
    audit_wc_est_mkt_value_PT_weekly,
    audit_wd_est_mkt_value_AG_weekly,
    audit_wd_est_mkt_value_AU_weekly,
    audit_wd_est_mkt_value_PD_weekly,
    audit_wd_est_mkt_value_PT_weekly,
    audit_yc_est_mkt_value_AG_weekly,
    audit_yc_est_mkt_value_AU_weekly,
    audit_yc_est_mkt_value_PD_weekly,
    audit_yc_est_mkt_value_PT_weekly,
    audit_yd_est_mkt_value_AG_weekly,
    audit_yd_est_mkt_value_AU_weekly,
    audit_yd_est_mkt_value_PD_weekly,
    audit_yd_est_mkt_value_PT_weekly,
    audit_yr_est_mkt_value_AG_weekly,
    audit_yr_est_mkt_value_AU_weekly,
    audit_yr_est_mkt_value_PD_weekly,
    audit_yr_est_mkt_value_PT_weekly,
    audit_wc_est_mkt_value_AG_month_fix,
    audit_wc_est_mkt_value_AU_month_fix,
    audit_wc_est_mkt_value_PD_month_fix,
    audit_wc_est_mkt_value_PT_month_fix,
    audit_wd_est_mkt_value_AG_month_fix,
    audit_wd_est_mkt_value_AU_month_fix,
    audit_wd_est_mkt_value_PD_month_fix,
    audit_wd_est_mkt_value_PT_month_fix,
    audit_yc_est_mkt_value_AG_month_fix,
    audit_yc_est_mkt_value_AU_month_fix,
    audit_yc_est_mkt_value_PD_month_fix,
    audit_yc_est_mkt_value_PT_month_fix,
    audit_yd_est_mkt_value_AG_month_fix,
    audit_yd_est_mkt_value_AU_month_fix,
    audit_yd_est_mkt_value_PD_month_fix,
    audit_yd_est_mkt_value_PT_month_fix,
    audit_yr_est_mkt_value_AG_month_fix,
    audit_yr_est_mkt_value_AU_month_fix,
    audit_yr_est_mkt_value_PD_month_fix,
    audit_yr_est_mkt_value_PT_month_fix,
    audit_wc_est_mkt_value_AG_settlement,
    audit_wc_est_mkt_value_AU_settlement,
    audit_wc_est_mkt_value_PD_settlement,
    audit_wc_est_mkt_value_PT_settlement,
    audit_wd_est_mkt_value_AG_settlement,
    audit_wd_est_mkt_value_AU_settlement,
    audit_wd_est_mkt_value_PD_settlement,
    audit_wd_est_mkt_value_PT_settlement,
    audit_yc_est_mkt_value_AG_settlement,
    audit_yc_est_mkt_value_AU_settlement,
    audit_yc_est_mkt_value_PD_settlement,
    audit_yc_est_mkt_value_PT_settlement,
    audit_yd_est_mkt_value_AG_settlement,
    audit_yd_est_mkt_value_AU_settlement,
    audit_yd_est_mkt_value_PD_settlement,
    audit_yd_est_mkt_value_PT_settlement,
    audit_yr_est_mkt_value_AG_settlement,
    audit_yr_est_mkt_value_AU_settlement,
    audit_yr_est_mkt_value_PD_settlement,
    audit_yr_est_mkt_value_PT_settlement,
    plan_quota_yearly,
    settlement_price_AG_value,
    settlement_price_PD_value,
    settlement_price_PT_value,
    settlement_price_AU_value,
    settlement_price__date,
    reps_base_pay_value,
    reps_bcr_value,
    alloy_tables_yc_ag_percents,
    alloy_tables_yc_au_percents,
    alloy_tables_yc_pd_percents,
    alloy_tables_yc_pt_percents,
    alloy_tables_yd_ag_percents,
    alloy_tables_yd_au_percents,
    alloy_tables_yd_pd_percents,
    alloy_tables_yd_pt_percents,
    alloy_tables_yr_ag_percents,
    alloy_tables_yr_au_percents,
    alloy_tables_yr_pd_percents,
    alloy_tables_yr_pt_percents,
    alloy_tables_wc_ag_percents,
    alloy_tables_wc_au_percents,
    alloy_tables_wc_pd_percents,
    alloy_tables_wc_pt_percents,
    alloy_tables_wd_ag_percents,
    alloy_tables_wd_au_percents,
    alloy_tables_wd_pd_percents,
    alloy_tables_wd_pt_percents,
    melt_potnumber,
    melt_refinery,
    melt_assay_pd_market_value,
    melt_assay_pt_market_value,
    melt_assay_au_market_value,
    melt_assay_ag_market_value,
    rafinery_assay_ag_grams,
    rafinery_assay_au_grams,
    rafinery_assay_pd_grams,
    rafinery_assay_pt_grams,
    rafinery_assay_ag_dollar_per_gram_avg,
    rafinery_assay_au_dollar_per_gram_avg,
    rafinery_assay_pd_dollar_per_gram_avg,
    rafinery_assay_pt_dollar_per_gram_avg,
    rafinery_assay_pt_dollars,
    rafinery_assay_pd_dollars,
    rafinery_assay_au_dollars,
    rafinery_assay_ag_dollars,
    
    NULL as refinery_est_mkt_value_AG_settlement,
    NULL as refinery_est_mkt_value_AU_settlement,
    NULL as refinery_est_mkt_value_PD_settlement,
    NULL as refinery_est_mkt_value_PT_settlement,

    NULL as rafinery_assay_AG_dollar_per_gram_without_terms_avg,
    NULL as rafinery_assay_AU_dollar_per_gram_without_terms_avg,
    NULL as rafinery_assay_PD_dollar_per_gram_without_terms_avg,
    NULL as rafinery_assay_PT_dollar_per_gram_without_terms_avg,

    rafinery_assay_pt_dollars_without_terms,
    rafinery_assay_pd_dollars_without_terms,
    rafinery_assay_au_dollars_without_terms,
    rafinery_assay_ag_dollars_without_terms,

    rafinery_assay_fee,
    rafinery_market_value_at_fix,
    melt_ids,
    movements,
    offers_total_offer_assayed,
    offers_total_offer_melt_assayed,
    melt_assaypdweight,
    melt_assayptweight,
    melt_assayauweight,
    melt_assayagweight
  
  FROM LoadTable_REPORT_spendLogAudit;


END IF;


/* Audit of DB and report logic */
/* Produce an audit of DB and Report */
CALL `dummy.dummy.ETL_AUDIT_STAGING_offersConsolidated`(
  (SELECT start_date_ts FROM TEMP_dateSelector),
  (SELECT end_date_ts FROM TEMP_dateSelector)
  );


END;