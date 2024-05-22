import os
import json
import traceback
import logging
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

logger = logging.getLogger(__name__)

load_dotenv()

# credential
credential_datamart = Path(__file__).parent.parent.resolve() / str(os.getenv("BQ_CREDENTIAL"))
project_id_bq = os.getenv("BQ_DATAMART_PROJECT_ID")
bq_db = os.getenv("BQ_DB")

try:
    if os.getenv("IS_SERVICE_ACCOUNT_FROM_FILE").lower() == "true":
        logger.info("Reading BigQuery credentials from files - churn pipeline")
        credential_datamart = service_account.Credentials.from_service_account_file(
            credential_datamart
        )
    else:
        credential_datamart = service_account.Credentials.from_service_account_info(
            json.loads(credential_datamart)
        )
        logger.info("Credentials are not from files")
except Exception as e:
    logger.error(
        "query or credentials for retrieving dataset for churn pipeline is not available. error {e}, traceback {tr}".format(
        e=e,
        tr=traceback.format_exc()
        )
    )
    raise ValueError(
        "query or credentials for churn pipeline is not available, {}".format(e)
    )

SCRAP_CONFIG = {
    "APP_NAME": os.getenv("APP_NAME"),
    "COUNTRY": os.getenv("COUNTRY"),
    "APP_ID": str(os.getenv("APP_ID")),
}
BQ_CONFIG = {
    "CRED" : credential_datamart,
    "PROJECT" : project_id_bq,
    "DB" : bq_db
}
# reason / topic model path
# reason_model = Path(__file__).parent.parent.resolve() / str(os.getenv("MODEL_LDA"))
reason_model = str(os.getenv("MODEL_LDA"))
reason_map = Path(__file__).parent.parent.resolve() / str(os.getenv("REASON_MAP"))

# table
APP_STORE_SCRAPING_TABLE = os.getenv("APP_STORE_SCRAPING_TABLE")
APP_STORE_NEG_REASON_RESULT_TABLE = os.getenv("APP_STORE_NEG_REASON_RESULT_TABLE")
APP_STORE_LOG_TABLE = os.getenv("APP_STORE_LOG_TABLE")
APP_STORE_TOPIC_LOG_TABLE = os.getenv("APP_STORE_TOPIC_LOG_TABLE")
# job config
job_config_batch = bigquery.QueryJobConfig(
    # Run at batch priority, which won't count toward concurrent rate limit.
    priority=bigquery.QueryPriority.BATCH
)

job_config_large_rows = bigquery.QueryJobConfig(
    # Run at batch priority, which won't count toward concurrent rate limit.
    allow_large_results=True,
    priority=bigquery.QueryPriority.BATCH,
)
