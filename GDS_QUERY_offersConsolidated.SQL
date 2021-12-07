WITH parseDates AS

          (SELECT CASE WHEN PARSE_TIMESTAMP("%Y%m%d", @DS_START_DATE) > current_timestamp()
                    THEN TIMESTAMP_TRUNC(current_timestamp(), DAY)
                    ELSE PARSE_TIMESTAMP("%Y%m%d", @DS_START_DATE)
                    END AS start_date_ts,

               CASE WHEN PARSE_TIMESTAMP("%Y%m%d", @DS_END_DATE) > current_timestamp()
                    THEN TIMESTAMP_SUB(CAST(DATE_ADD(current_date(), INTERVAL 1 DAY) AS TIMESTAMP), INTERVAL 1 SECOND)
                    ELSE PARSE_TIMESTAMP("%Y%m%d %H:%M:%S", CONCAT(@DS_END_DATE, " 23:59:59"))
                    END AS end_date_ts

          ),

     dateSelector AS

          (SELECT start_date_ts,
                  CAST(start_date_ts AS datetime) AS start_date_dt,

                  end_date_ts,
                  CAST(end_date_ts AS datetime) AS end_date_dt
          
          FROM parseDates),
/*

     dateSelector AS

          (SELECT
                    CASE WHEN EXTRACT(DAYOFWEEK FROM start_date_ts) = 1
                    THEN TIMESTAMP_SUB(start_date_ts, INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts) = 2
                    THEN TIMESTAMP_SUB(start_date_ts, INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts) = 3
                    THEN TIMESTAMP_SUB(start_date_ts, INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts) = 4
                    THEN TIMESTAMP_SUB(start_date_ts, INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts) = 5
                    THEN TIMESTAMP_SUB(start_date_ts, INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts) = 6
                    THEN TIMESTAMP_SUB(start_date_ts, INTERVAL 6 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM start_date_ts) = 7
                    THEN start_date_ts
                    
                    ELSE NULL END AS start_date_ts,


                    CASE WHEN EXTRACT(DAYOFWEEK FROM end_date_ts ) = 1
                    THEN TIMESTAMP_ADD(end_date_ts , INTERVAL 5 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM end_date_ts ) = 2
                    THEN TIMESTAMP_ADD(end_date_ts , INTERVAL 4 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM end_date_ts ) = 3
                    THEN TIMESTAMP_ADD(end_date_ts, INTERVAL 3 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM end_date_ts ) = 4
                    THEN TIMESTAMP_ADD(end_date_ts, INTERVAL 2 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM end_date_ts ) = 5
                    THEN TIMESTAMP_ADD(end_date_ts, INTERVAL 1 DAY)

                    WHEN EXTRACT(DAYOFWEEK FROM end_date_ts ) = 6
                    THEN end_date_ts

                    WHEN EXTRACT(DAYOFWEEK FROM end_date_ts) = 7
                    THEN TIMESTAMP_ADD(end_date_ts, INTERVAL 6 DAY)

                    ELSE NULL END AS end_date_ts

          FROM parseDates


          ),

*/

     offersRaw AS

          (SELECT offers_tb.*,
                  reps_tb.name as reps_name,
               
                  CASE WHEN reps_tb.timezoneid IS NULL OR reps_tb.timezoneid = 'Unassigned'
                       THEN EXTRACT(DATETIME FROM offers_tb.offer_date)
                       ELSE EXTRACT(DATETIME FROM offers_tb.offer_date AT TIME ZONE reps_tb.timezoneid)
                  END as offer_date_dt_local_tz

           FROM `dummy.dummy.offers` as offers_tb

           LEFT JOIN `dummy.dummy.reps` as reps_tb

           ON offers_tb.rep_id = reps_tb.id

           WHERE offers_tb.offer_date BETWEEN (SELECT TIMESTAMP_SUB(start_date_ts, INTERVAL 1 DAY) FROM dateSelector)
                 AND (SELECT TIMESTAMP_ADD(end_date_ts, INTERVAL 1 DAY) FROM dateSelector)
                 AND offers_tb.status != 'NoVisit'
          
          ),

     



     offersConsolidated AS
          
          /* Get main offers considated data with straight forward joins and filtering with parameters */

          (SELECT 
                    offers_tb.wc_pd_grams AS wc_pd_grams, 
                    /* offers_tb.forex_rate AS forex_rate, */
                    offers_tb.yd_au_grams AS yd_au_grams, 
                    offers_tb.yd_pt_percents AS yd_pt_percents, 
                    offers_tb.yd_pd_grams AS yd_pd_grams, 
                    offers_tb.territory_id AS territory_id, 
                    offers_tb.wc_au_grams AS wc_au_grams, 
                    offers_tb.wd_ag_grams AS wd_ag_grams, 
                    offers_tb.wd_weight_loss AS wd_weight_loss, 
                    /* offers_tb.spendlog_audit_id AS spendlog_audit_id, */
                    offers_tb.yr_ag_percents AS yr_ag_percents, 
                    /* offers_tb.high_nickel_input_values_array_id AS high_nickel_input_values_array_id, */
                    offers_tb.yr_pd_grams AS yr_pd_grams, 
                    /* offers_tb.extendedstatus AS extendedstatus, */
                    offers_tb.wd_au_percents AS wd_au_percents, 
                    /* offers_tb.antimoneylaunderingdata_id AS antimoneylaunderingdata_id, */
                    offers_tb.yc_pd_grams AS yc_pd_grams, 
                    offers_tb.yr_ag_grams AS yr_ag_grams, 
                    offers_tb.discount AS discount, 
                    offers_tb.wc_pd_percents AS wc_pd_percents, 
                    offers_tb.yd_ag_percents AS yd_ag_percents, 
                    offers_tb.yc_au_grams AS yc_au_grams, 
                    offers_tb.pt_per_oz AS pt_per_oz, 
                    offers_tb.wc_pt_percents AS wc_pt_percents, 
                    offers_tb.ag_dollars AS ag_dollars, 
                    offers_tb.yd_au_percents AS yd_au_percents, 
                    /* offers_tb.longitude AS longitude, */
                    offers_tb.pt_dollars AS pt_dollars, 
                    /* offers_tb.refineinfoforwhite_id AS refineinfoforwhite_id, */
                    offers_tb.yr_pt_grams AS yr_pt_grams, 
                    /* offers_tb.account_data AS account_data, */
                    offers_tb.outcome_details AS outcome_details, 
                    offers_tb.id AS id, 
                    offers_tb.pt_per_g AS pt_per_g, 
                    offers_tb.offer_currency AS offer_currency, 
                    offers_tb.yd_pt_grams AS yd_pt_grams, 
                    /* offers_tb.latitude AS latitude, */
                    offers_tb.yr_grams AS yr_grams, 
                    offers_tb.wc_ag_percents AS wc_ag_percents, 
                    offers_tb.wc_weight_loss AS wc_weight_loss, 
                    offers_tb.yd_pd_percents AS yd_pd_percents, 
                    offers_tb.au_adjustment_dollars AS au_adjustment_dollars, 
                    offers_tb.yd_weight_loss AS yd_weight_loss, 
                    offers_tb.au_dollars AS au_dollars, 
                    /* offers_tb.ag_adjustment_dollars AS ag_adjustment_dollars, */
                    /* offers_tb._sdc_table_version AS _sdc_table_version, */
                    /* offers_tb.uuid AS uuid, */
                    offers_tb.yr_weight_loss AS yr_weight_loss, 
                    offers_tb.melt_id AS melt_id, 
                    offers_tb.yc_pd_percents AS yc_pd_percents, 
                    offers_tb.au_per_g AS au_per_g, 
                    offers_tb.ag_per_g AS ag_per_g, 
                    offers_tb.yd_ag_grams AS yd_ag_grams, 
                    /* offers_tb.forex_adjustment AS forex_adjustment, */
                    offers_tb.updated_at AS updated_at, 
                    offers_tb.status AS status, 
                    offers_tb.total_offer_in_offer_currency AS total_offer_in_offer_currency, 
                    offers_tb.rep_id AS rep_id, 
                    offers_tb.wd_pt_grams AS wd_pt_grams, 
                    /* offers_tb.refineinfoforyellow_id AS refineinfoforyellow_id, */
                    offers_tb.yc_grams AS yc_grams, 
                    offers_tb.yr_au_grams AS yr_au_grams, 
                    offers_tb.wd_grams AS wd_grams, 
                    /* offers_tb._sdc_received_at AS _sdc_received_at, */
                    offers_tb.visit_id AS visit_id, 
                    /* offers_tb._sdc_sequence AS _sdc_sequence, */
                    offers_tb.pd_adjustment_dollars AS pd_adjustment_dollars, 
                    offers_tb.yr_pt_percents AS yr_pt_percents, 
                    offers_tb.au_per_oz AS au_per_oz, 
                    /* offers_tb.signature_url AS signature_url, */
                    CASE WHEN status = 'Accepted' THEN offers_tb.total_offer ELSE 0 END AS total_offer, 
                    offers_tb.wd_au_grams AS wd_au_grams, 
                    /* offers_tb.photo_urls AS photo_urls, */
                    offers_tb.wd_pd_grams AS wd_pd_grams, 
                    offers_tb.wc_au_percents AS wc_au_percents, 
                    offers_tb.created_at AS created_at, 
                    offers_tb.wc_ag_grams AS wc_ag_grams, 
                    offers_tb.wc_pt_grams AS wc_pt_grams, 
                    offers_tb.pd_per_oz AS pd_per_oz, 
                    offers_tb.yc_weight_loss AS yc_weight_loss, 
                    offers_tb.ag_per_oz AS ag_per_oz, 
                    offers_tb.yc_ag_percents AS yc_ag_percents, 
                    offers_tb.wd_pd_percents AS wd_pd_percents, 
                    offers_tb.pd_per_g AS pd_per_g, 
                    offers_tb.yc_pt_percents AS yc_pt_percents, 
                    offers_tb.yr_pd_percents AS yr_pd_percents, 
                    offers_tb.yc_ag_grams AS yc_ag_grams, 
                    offers_tb.yd_grams AS yd_grams, 
                    offers_tb.account_id AS account_id, 
                    /* offers_tb._sdc_batched_at AS _sdc_batched_at, */
                    offers_tb.offer_date AS offer_date, 
                    /* offers_tb._sdc_extracted_at AS _sdc_extracted_at, */
                    /* offers_tb.input_values_array_id AS input_values_array_id, */
                    offers_tb.yc_au_percents AS yc_au_percents, 
                    offers_tb.wd_pt_percents AS wd_pt_percents, 
                    offers_tb.yc_pt_grams AS yc_pt_grams, 
                    offers_tb.yr_au_percents AS yr_au_percents, 
                    offers_tb.pd_dollars AS pd_dollars, 
                    offers_tb.wc_grams AS wc_grams, 
                    offers_tb.pt_adjustment_dollars AS pt_adjustment_dollars, 
                    offers_tb.wd_ag_percents AS wd_ag_percents, 

                    /* Price per grams */
                    dummy.bigDecimal_division(CASE WHEN offers_tb.au_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.au_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL)) AS au_per_grams,
                    dummy.bigDecimal_division(CASE WHEN offers_tb.ag_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.ag_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL)) AS ag_per_grams,
                    dummy.bigDecimal_division(CASE WHEN offers_tb.pd_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pd_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL)) AS pd_per_grams,
                    dummy.bigDecimal_division(CASE WHEN offers_tb.pt_per_oz IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(offers_tb.pt_per_oz AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL)) AS pt_per_grams,

                    

                    /* Dates */
                    EXTRACT(Year FROM offers_tb.offer_date_dt_local_tz) AS offer_year,                    
                    EXTRACT(Month FROM offers_tb.offer_date_dt_local_tz) AS offer_month,
                    EXTRACT(Week FROM offers_tb.offer_date_dt_local_tz) AS offer_week,


                    /* Week calculation */
                    CASE WHEN EXTRACT(DAYOFWEEK FROM offers_tb.offer_date_dt_local_tz) = 1 THEN CAST(CAST(TIMESTAMP_SUB(offers_tb.offer_date_dt_local_tz, INTERVAL 1 DAY) as Date) as Timestamp)
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

                    CONCAT("Y", EXTRACT(Year from offer_date), "-W", EXTRACT(Week from offer_date)) as offer_year_week,
                    CONCAT("Y", EXTRACT(Year from offer_date), "-M", EXTRACT(Month from offer_date)) as offer_year_month,

                    offers_tb.reps_name AS reps_name,
                    accounts_tb.practice_name as accounts_practice_name,
                    accounts_tb.phone as accounts_phone,
                    CASE WHEN accounts_tb.phone IS NOT NULL AND accounts_tb.phone != ""
                         THEN CONCAT("(", SUBSTR(accounts_tb.phone, 0, 3), ") ", SUBSTR(accounts_tb.phone, 3, 3), "-", SUBSTR(accounts_tb.phone, 6))
                         ELSE accounts_tb.phone END as accounts_phone_formated,
                    
                    CASE WHEN places_tb.city IS NULL OR places_tb.city = "" THEN "N/A" ELSE places_tb.city END as places_city,
                    CASE WHEN places_tb.state IS NULL OR places_tb.state = "" THEN "N/A" ELSE places_tb.state END as places_state,
                    places_tb.address as places_address,
                    places_tb.latitude as places_latitude__de,
                    places_tb.longitude as places_longitude__de,
                    CONCAT(places_tb.latitude, ", ", places_tb.longitude) as places_latitude_longitude,
                    CONCAT(places_tb.address, ", ", places_tb.city, ", ", places_tb.state) as places_fullAddress,



                    /* CASE WHEN territory_tb.name IS NULL THEN "N/A" ELSE territory_tb.name END as territory_name, */
                    
                    CASE WHEN accounts_tb.territory_id IS NULL
                         THEN CASE WHEN offers_tb.territory_id IS NULL
                                   THEN -1
                                   ELSE offers_tb.territory_id
                              END
                         ELSE accounts_tb.territory_id
                    END as dim_territory_id,
                    
                    CAST(offers_tb.offer_date_dt_local_tz AS DATE) AS offer_date_tz_adjusted_no_time,
                    FORMAT_TIMESTAMP("%d/%m/%Y", offers_tb.offer_date) as offer_date_no_time_string,
                    CASE WHEN pickuprequest_tb.offer_id IS NULL THEN False ELSE True END as is_pickuprequest
                              
                    
          FROM offersRaw as offers_tb
          
          /*
          LEFT JOIN `dummy.dummy.reps` as reps_tb
          ON offers_tb.rep_id = reps_tb.id */
          
          LEFT JOIN `dummy.dummy.PickupRequest` as pickuprequest_tb
          ON offers_tb.id = pickuprequest_tb.offer_id 
               
          LEFT JOIN `dummy.dummy.accounts` as accounts_tb
          ON offers_tb.account_id = accounts_tb.id

          /*
          LEFT JOIN `dummy.dummy.Territory` as territory_tb
          ON accounts_tb.territory_id = territory_tb.id
          */
          
          LEFT JOIN `dummy.dummy.places` as places_tb
          ON accounts_tb.place_id = places_tb.id

          /* PARAMETERS */
          WHERE offers_tb.offer_date_dt_local_tz BETWEEN (SELECT start_date_dt FROM dateSelector)
              AND (SELECT end_date_dt FROM dateSelector)
          
          AND
          (offers_tb.status = @offer_status OR (@offer_status = "None" AND offers_tb.status != 'NoVisit')) 
          
          ),   

     
     /* Prepare xigdata format for later joining. Also logic to make sure morning or evening is taken on max date */
     xigdatas AS
          
     (SELECT CAST(date AS DATETIME) as xigdatas_date_dt,
               symbol as xigdatas_symbol,
               CASE WHEN period = 'Morning' THEN 0 ELSE 1 END as xigdatas_period,
               MAX(value) as xigdatas_value

     FROM `dummy.dummy.xigdatas`

     WHERE date = (SELECT max(date) FROM `dummy.dummy.xigdatas` WHERE currency = 'USD' AND symbol = 'AU')
          AND currency = 'USD'
          AND symbol = 'AU'
     
     GROUP BY xigdatas_date_dt, xigdatas_symbol, xigdatas_period

     UNION ALL

     SELECT CAST(date AS DATETIME) as xigdatas_date_dt,
               symbol as xigdatas_symbol,
               CASE WHEN period = 'Morning' THEN 0 ELSE 1 END as xigdatas_period,
               MAX(value) as xigdatas_value

     FROM `dummy.dummy.xigdatas`

     WHERE date = (SELECT max(date) FROM `dummy.dummy.xigdatas` WHERE currency = 'USD' AND symbol = 'AG')
          AND currency = 'USD'
          AND symbol = 'AG'
     
     GROUP BY xigdatas_date_dt, xigdatas_symbol, xigdatas_period

     UNION ALL

     SELECT CAST(date AS DATETIME) as xigdatas_date_dt,
               symbol as xigdatas_symbol,
               CASE WHEN period = 'Morning' THEN 0 ELSE 1 END as xigdatas_period,
               MAX(value) as xigdatas_value

     FROM `dummy.dummy.xigdatas`

     WHERE date = (SELECT max(date) FROM `dummy.dummy.xigdatas` WHERE currency = 'USD' AND symbol = 'PD')
          AND currency = 'USD'
          AND symbol = 'PD'
     
     GROUP BY xigdatas_date_dt, xigdatas_symbol, xigdatas_period

     UNION ALL

     SELECT CAST(date AS DATETIME) as xigdatas_date_dt,
               symbol as xigdatas_symbol,
               CASE WHEN period = 'Morning' THEN 0 ELSE 1 END as xigdatas_period,
               MAX(value) as xigdatas_value

     FROM `dummy.dummy.xigdatas`

     WHERE date = (SELECT max(date) FROM `dummy.dummy.xigdatas` WHERE currency = 'USD' AND symbol = 'PT')
          AND currency = 'USD'
          AND symbol = 'PT'
     
     GROUP BY xigdatas_date_dt, xigdatas_symbol, xigdatas_period



     )

     /* Account last purchase + Account creation date implied from first ever visit.
     This will be used for calculating account last purchase (or first visit if no purchases made) buckets on GDS */

     /*
     
     accounts_last_purchase AS


     (SELECT offers_tb.account_id as account_id,

          offers_tb.id as offer_id,

          TIMESTAMP_DIFF(
                    dateSelector_tb.end_date_ts,
                    offers_tb.offer_date,
                    DAY
                    ) as daysToEndDate,  

               SUM(TIMESTAMP_DIFF(
                    dateSelector_tb.end_date_ts,
                    offers_tb.offer_date,
                    DAY
                    )) OVER (partition by offers_tb.account_id ORDER BY offers_tb.account_id, offers_tb.id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
                    
                    AS daysToEndDate_sum_previous_current_row


     FROM 
     
     (SELECT account_id,
               id,
               offer_date

          FROM offersRaw
          
          UNION ALL

          SELECT main_tb.account_id as account_id,
               main_tb.id as id,
               offer_dates_join_tb.offer_date as offer_date
          
          FROM

          (SELECT account_id,
               max(id) as id,
          
          FROM `dummy.dummy.offers`

          WHERE offer_date < (SELECT start_date_ts FROM dateSelector)

          GROUP BY account_id

          ) as main_tb

          LEFT JOIN `dummy.dummy.offers` as offer_dates_join_tb

          ON main_tb.id = offer_dates_join_tb.id
          
          ) AS offers_tb

     CROSS JOIN (SELECT end_date_ts FROM dateSelector) as dateSelector_tb

     WHERE account_id IS NOT NULL

     */

     /* GROUP BY offers_tb.account_id, offers_tb.id, offers_tb.offer_date, dateSelector_tb.end_date_ts )*/


/*      accounts_first_visit_no_purchases AS

          (SELECT offers_tb.account_id as account_id,

               offers_tb.id as offer_id,

               TIMESTAMP_DIFF(
                         dateSelector_tb.end_date_ts,
                         offers_tb.offer_date,
                         DAY
                         ) as daysToEndDate,  

               SUM(TIMESTAMP_DIFF(
                         dateSelector_tb.end_date_ts,
                         offers_tb.offer_date,
                         DAY
                         )) OVER (partition by offers_tb.account_id ORDER BY offers_tb.account_id, offers_tb.id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
                         
                         AS daysToEndDate_sum_previous_current_row

                    FROM offersRaw as offers_tb

                    CROSS JOIN (SELECT end_date_ts FROM dateSelector) as dateSelector_tb

                    LEFT JOIN accounts_last_purchase as accounts_last_purchase_tb

                    ON offers_tb.account_id = accounts_last_purchase_tb.account_id
                         
                    WHERE accounts_last_purchase_tb.account_id IS NULL
                         AND offers_tb.account_id IS NOT NULL AND offers_tb.status != "Accepted"

                    /* GROUP BY offers_tb.account_id, offers_tb.id, offers_tb.offer_date, dateSelector_tb.end_date_ts )

*/

SELECT offersConsolidated_tb.*,
     history_territory_tb.name  as territory_name,
     
     CONCAT(CAST(offersConsolidated_tb.offer_date_start_week as Date), " - ", CAST(offersConsolidated_tb.offer_date_end_week as Date)) as week_start_end,

     /* (CASE WHEN daysToEndDate_sum_previous_current_row IS NULL THEN 0 ELSE daysToEndDate_sum_previous_current_row END - CASE WHEN daysToEndDate IS NULL THEN 0 ELSE daysToEndDate END) AS last_purchase_days, */

     CASE WHEN lastAcceptedOffer_tb.prev_accepted_offer_date IS NULL THEN -1
            ELSE TIMESTAMP_DIFF(lastAcceptedOffer_tb.offer_date, lastAcceptedOffer_tb.prev_accepted_offer_date, DAY)
            END AS last_purchase_days,

     /* xigdatas fields */
     dummy.bigDecimal_division(CASE WHEN xigdatas_value_AG IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(xigdatas_value_AG AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL)) as xigdatas_value_AG,
     dummy.bigDecimal_division(CASE WHEN xigdatas_value_AU IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(xigdatas_value_AU AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL)) as xigdatas_value_AU,
     dummy.bigDecimal_division(CASE WHEN xigdatas_value_PD IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(xigdatas_value_PD AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL)) as xigdatas_value_PD,
     dummy.bigDecimal_division(CASE WHEN xigdatas_value_PT IS NULL THEN CAST(0 AS BIGDECIMAL) ELSE CAST(xigdatas_value_PT AS BIGDECIMAL) END, CAST(31.1034768 AS BIGDECIMAL)) as xigdatas_value_PT

FROM offersConsolidated as offersConsolidated_tb

/* Join SCD Territory data */
LEFT JOIN `dummy.dummy.history_territory` as history_territory_tb
ON offersConsolidated_tb.dim_territory_id = history_territory_tb.id
AND offersConsolidated_tb.offer_date_start_week >= history_territory_tb.start_ts
AND (history_territory_tb.end_ts IS NULL OR offersConsolidated_tb.offer_date_end_week < history_territory_tb.end_ts)




/* Join account last purchase data */
LEFT JOIN `dummy.dummy.TRANSFORMED_offers_lastAcceptedOffer` as lastAcceptedOffer_tb
ON offersConsolidated_tb.id = lastAcceptedOffer_tb.id
   AND offersConsolidated_tb.account_id = lastAcceptedOffer_tb.account_id


/* Join account last purchase (or first offer if no purchases ever made) timestamps / day durations
for GDS metric buckets  0-18, 18-24, 24-36, 36 or more */

/*
LEFT JOIN accounts_last_purchase AS accounts_last_purchase_tb

ON offersConsolidated_tb.account_id = accounts_last_purchase_tb.account_id
     AND offersConsolidated_tb.id = accounts_last_purchase_tb.offer_id
     */


/* Join xigdatas */
CROSS JOIN (SELECT max(xigdatas_value) as xigdatas_value_AG FROM xigdatas WHERE xigdatas_period = (SELECT max(xigdatas_period) FROM xigdatas WHERE xigdatas_symbol = 'AG') AND xigdatas_symbol = 'AG')

CROSS JOIN (SELECT max(xigdatas_value) as xigdatas_value_AU FROM xigdatas WHERE xigdatas_period = (SELECT max(xigdatas_period) FROM xigdatas WHERE xigdatas_symbol = 'AU') AND xigdatas_symbol = 'AU')

CROSS JOIN (SELECT max(xigdatas_value) as xigdatas_value_PD FROM xigdatas WHERE xigdatas_period = (SELECT max(xigdatas_period) FROM xigdatas WHERE xigdatas_symbol = 'PD') AND xigdatas_symbol = 'PD')

CROSS JOIN (SELECT max(xigdatas_value) as xigdatas_value_PT FROM xigdatas WHERE xigdatas_period = (SELECT max(xigdatas_period) FROM xigdatas WHERE xigdatas_symbol = 'PT') AND xigdatas_symbol = 'PT')
;
