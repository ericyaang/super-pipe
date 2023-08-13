import os
from datetime import datetime

from dotenv import load_dotenv
from google.cloud.bigquery import ExternalConfig
from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_create_table

from core.duckdb.utils import handle_path_date

load_dotenv()

BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BUCKET_PATH_SILVER = os.getenv("GCS_BUCKET_PATH_SILVER")
BUCKET_PATH_GOLD = os.getenv("GCS_BUCKET_PATH_GOLD")
BUCKET_LOCATION = os.getenv("GCS_BUCKET_LOCATION")
BQ_DATASET_NAME = os.getenv("BQ_DATASET_NAME")


@flow(name="Upload the golden data from today to BigQuery")
def gcs_to_bigquery(date: str = "today") -> None:
    """The main function for creating an external table in BQ from GCS"""

    select_date = handle_path_date(date)
    gcp_credentials_block = GcpCredentials.load("default")
    external_table_options = ExternalConfig("PARQUET")
    external_table_options.autodetect = True
    external_table_options.source_uris = [
        f"gs://{BUCKET_NAME}/{BUCKET_PATH_GOLD}/{select_date}/*.parquet"
    ]  # ALTERAR AQUI DEPOIS

    if date == "today" or date == "all":
        timestamp = datetime.now().strftime("%Y_%m_%d")
    else:
        timestamp = date.replace("-", "_")

    result = bigquery_create_table(
        dataset=BQ_DATASET_NAME,
        table=f"gold_{timestamp}",
        external_config=external_table_options,
        gcp_credentials=gcp_credentials_block,
        location=BUCKET_LOCATION,  # <---- Location must be the sames as the bucket
    )
    return result


# gcs_to_bigquery(date='2023-07-27') #'YYYY-MM-DD'
if __name__ == "__main__":
    gcs_to_bigquery("2023-08-05")
