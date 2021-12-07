import pandas as pd
#import logging
from google.cloud import bigquery

path = "/tmp"    

#logging.basicConfig(filename = path + '/run_offers_last_accepted.log', level=logging.INFO)


def run_offers_last_accepted(*args):

    #logging.info("Processing offers...")
    print("Processing offers...")

    bqclient = bigquery.Client()

    querySQL = """WITH selectedOffers
                    AS
                    (SELECT account_id, id, offer_date, status, True as selected
                    FROM `dummy.dummy`
                    WHERE offer_date >= TIMESTAMP_SUB(current_timestamp(), INTERVAL 14 DAY) AND status != 'NoVisit' AND account_id IS NOT NULL
                    ORDER BY account_id ASC, id ASC)


                    SELECT * FROM

                        (SELECT main.account_id as account_id,
                            main.id as id,
                            offers_tb.offer_date as offer_date,
                            offers_tb.status as status,
                            False as selected

                        FROM

                        (SELECT account_id, max(id) as id
                        FROM `dummy.dummy`
                        WHERE offer_date < TIMESTAMP_SUB(current_timestamp(), INTERVAL 14 DAY) AND status = 'Accepted' AND account_id IS NOT NULL
                            AND account_id IN (SELECT account_id FROM selectedOffers GROUP BY account_id)
                        GROUP BY account_id
                        ORDER BY account_id ASC, id ASC) AS main
                        LEFT JOIN `dummy.dummy` as offers_tb

                        ON main.account_id = offers_tb.account_id
                            AND main.id = offers_tb.id

                        UNION ALL

                        SELECT account_id, id, offer_date, status, selected
                        FROM selectedOffers)

                        ORDER BY account_id ASC, id ASC"""


                        

    InitialQuerySQL = """SELECT * FROM

    (SELECT main.account_id as account_id,
        main.id as id,
        offers_tb.offer_date as offer_date,
        offers_tb.status as status,
        False as selected

    FROM

    (SELECT account_id, max(id) as id
    FROM `dummy.dummy`
    WHERE offer_date < TIMESTAMP_SUB(current_timestamp(), INTERVAL 365 * 1 DAY) AND status = 'Accepted' AND account_id IS NOT NULL
    GROUP BY account_id
    ORDER BY account_id ASC, id ASC) AS main
    LEFT JOIN `dummy.dummy` as offers_tb

    ON main.account_id = offers_tb.account_id
        AND main.id = offers_tb.id

    UNION ALL

    SELECT account_id, id, offer_date, status, True as selected
    FROM `dummy.dummy`
    WHERE offer_date BETWEEN TIMESTAMP_SUB(current_timestamp(), INTERVAL 365 * 1 DAY)
    AND TIMESTAMP_SUB(current_timestamp(), INTERVAL 365 * 0 DAY)
    AND status != 'NoVisit' AND account_id IS NOT NULL
    ORDER BY account_id ASC, id ASC)

    ORDER BY account_id ASC, id ASC

    """

    data = (
        bqclient.query(querySQL)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )

    #data = pd.read_csv("results-20210723-002107.csv")

    #Function definition
    def transformFunction(row) -> int:
        returnValue = data.loc[(data['account_id'] == row['account_id']) & (data['status'] == 'Accepted') & (data['offer_date'] < row['offer_date']), 'offer_date']
        returnValue = max(returnValue) if len(returnValue) > 0 else None
        
        return returnValue


    #Convert to proper datetime type
    def transformDate(x):
        try:
            dateValue = pd.to_datetime(x, utc=True)
            return dateValue
        except:
            return None

    data['offer_date'] = data.apply(lambda x: transformDate(x['offer_date']), axis = 1)

    #Filter out dates that could not be converted
    data = data[~data['offer_date'].isna()]

    #Transform using function
    data['prev_accepted_offer_date'] = data.apply(lambda x: transformFunction(x), axis = 1)

    #logging.info("Processing offers... Done")
    print("Processing offers... Done")


    #Remove old ids
    #logging.info("Deleting old ids...")
    print("Deleting old ids...")

    idsToRemove = ", ".join([str(x) for x in data[data['selected'] == True]['id'].tolist()])
    queryDeleteOldIdsSQL = """DELETE `dummy.dummy.TRANSFORMED_offers_lastAcceptedOffer` WHERE id IN ({})""".format(idsToRemove)
    del idsToRemove
    bqclient.query(queryDeleteOldIdsSQL).result()
    
    #logging.info("Deleting old ids... Done")
    print("Deleting old ids... Done")


    #Loading data to bigquery
    #logging.info("Loading data into BQ...")
    print("Loading data into BQ...")
    data[data['selected'] == True][[col for col in data.columns if col != 'selected']].to_gbq(
        "dummy.dummy.TRANSFORMED_offers_lastAcceptedOffer",
        project_id = "dummy",
        if_exists = 'append'
        )
    #logging.info("Loading data into BQ... Done")
    print("Loading data into BQ... Done")



if __name__ == '__main__':
    run_offers_last_accepted()




