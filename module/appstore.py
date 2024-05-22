import logging
import warnings
import pytz
import hashlib
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime
from app_store_scraper import AppStore
from module.as_config import SCRAP_CONFIG, BQ_CONFIG, APP_STORE_SCRAPING_TABLE, APP_STORE_LOG_TABLE
from module.bq_connection import BQConnection


logger = logging.getLogger("App Store Scrapper")
warnings.filterwarnings("ignore")

class APPStoreScraper(object):
    def __init__(self,
                 date_filter):
        self.credential_datamart = BQ_CONFIG["CRED"]
        self.project_id = BQ_CONFIG["PROJECT"]
        self.dataset = BQ_CONFIG["DB"]
        self.table = APP_STORE_SCRAPING_TABLE
        self.log_table = APP_STORE_LOG_TABLE
        self.scrap_config = SCRAP_CONFIG
        self.bq = BQConnection()
        self.date_filter = pd.to_datetime(date_filter)

    def scrape_data(self):
        jmo = AppStore(country=self.scrap_config["COUNTRY"], app_name=self.scrap_config["APP_NAME"], app_id = self.scrap_config["APP_ID"])
        logging.debug("Connecting to appstore scrapper")
        jmo.review(after=self.date_filter)
        jmodf = pd.DataFrame(np.array(jmo.reviews),columns=['review'])
        jmodf = jmodf.join(pd.DataFrame(jmodf.pop('review').tolist()))
        logging.info("Successfuly scraped data after {}".format(self.date_filter))
        jakarta_tz = pytz.timezone("Asia/Jakarta")
        jakarta_time = datetime.now(jakarta_tz)
        jmodf["created_at"] = jakarta_time
        # Convert datetime to string
        datetime_str = str(jakarta_time)

        # Calculate the SHA-256 hash
        hash_object = hashlib.sha256(datetime_str.encode())
        hash_hex = hash_object.hexdigest()
        jmodf["job_id"] = hash_hex
        job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField("date", "DATETIME"),
                    bigquery.SchemaField("rating", "FLOAT"),
                    bigquery.SchemaField("created_at", "DATETIME"),
                    bigquery.SchemaField("job_id", "STRING"),
                ],
            )
        
        self.bq.to_bq(
                jmodf[["job_id", 'date', 'review', 'rating', 'userName',
       'title','created_at']],
                self.dataset + "." + self.table,
                self.credential_datamart,
                self.project_id,
                job_config,
            )
        logging.info(
            "Finished inserting rows of scrapped data at {}".format(
                datetime.today()
            )
        )
       
        
        log_data = {'created_at': [jakarta_time], 'job_id': [hash_hex]}
        log_data = pd.DataFrame(log_data)
        job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField("created_at", "DATETIME"),
                    bigquery.SchemaField("job_id", "STRING")
                ],
            )
        self.bq.to_bq(
                log_data,
                self.dataset + "." + self.log_table,
                self.credential_datamart,
                self.project_id,
                job_config,
            )
        logging.info(
            "Finished inserting rows of log data at {}".format(
                datetime.today()
            )
        )
       