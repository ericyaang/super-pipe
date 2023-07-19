import tempfile
import uuid
from datetime import datetime
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect_gcp.cloud_storage import GcsBucket
from dotenv import load_dotenv
import os

load_dotenv()

GCS_BUCKET_PATH = os.getenv("GCS_BUCKET_PATH_TEST")

@task(name="Convert-Data-to-Pandas-DataFrame")
def convert_data_to_pandas_dataframe(data: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(data=data)

@task(name="Load-DF-to-Parquet-and-Upload-to-GCS")
def load_df_to_parquet_and_upload_to_gcs(df, serialization_format: str = 'parquet') -> None:
    gcs_bucket_block = GcsBucket.load("gcp-bucket-corner")


    DATETIME_UPLOADED = datetime.now().strftime("%d%m%Y_%H%M%S")

    destination_gcs_path = (
        f"{GCS_BUCKET_PATH}/{DATETIME_UPLOADED}/{datetime.now().isoformat()}"
    )
    gcs_bucket_block.upload_from_dataframe(
        df, to_path=destination_gcs_path, serialization_format=serialization_format
    )
    return destination_gcs_path

@flow(name="Load-to-Parquet-and-Upload-to-GCS")
def load_to_parquet_and_upload_to_gcs(data: list[dict]) -> str:
    logger = get_run_logger()

    logger.info("---Starting Load to Parquet and Upload to GCS---")

    logger.info("Converting data to Pandas DataFrame")
    df = convert_data_to_pandas_dataframe(data=data)

    logger.info("Loading DF to Parquet and Uploading to GCS")
    gcs_filename = load_df_to_parquet_and_upload_to_gcs(df=df)

    logger.info("---Finished Loading Data to Parquet File in GCS---")

    return gcs_filename

if __name__ == "__main__":
    load_to_parquet_and_upload_to_gcs()