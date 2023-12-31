from datetime import datetime
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect_gcp.cloud_storage import GcsBucket
from core.cornershop.main import fetch_corner_shop_data
from time import perf_counter


@task(name="Convert-Data-to-Pandas-DataFrame")
def convert_data_to_pandas_dataframe(data: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(data=data)


@task(name="Load-DF-to-Parquet-and-Upload-to-GCS")
def load_df_to_parquet_and_upload_to_gcs(
    df, serialization_format: str = "parquet"
) -> None:
    gcs_bucket_block = GcsBucket.load("default")  # <--- your bucket block name

    DATETIME_UPLOADED = datetime.now().strftime("year=%Y/month=%m/day=%d")
    HOUR_UPLOADED = datetime.now().strftime("%H%M%S")
    SEARCH_TERM = df["search_term"][0]
    CITY = df["store_city"][0]

    destination_gcs_path = f"bronze_teste/{DATETIME_UPLOADED}/term={SEARCH_TERM}/city={CITY}/{SEARCH_TERM}_{HOUR_UPLOADED}"  # example: 'bronze/year=2023/month=07/day=21/term=twix/city=FLN/twix_195250.parquet'
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


@flow(name="CornerShop-Fetch-Data-to-GCS", log_prints=True)
def fetch_data_to_gcs(items: list):
    logger = get_run_logger()

    logger.info("---Starting CornerShop Ingestion Flow---")
    start_time = perf_counter()

    for item in items:
        data = fetch_corner_shop_data(item)
        # Skip to the next item if no data is returned
        if not data:
            logger.info(f"No data returned for {item}, skipping to next item.")
            continue
        logger.info(
            f"Loading data for {item} into Parquet and then uploading to Google Cloud Storage"
        )
        load_to_parquet_and_upload_to_gcs(data)
    end_time = perf_counter()
    total_time = end_time - start_time
    logger.info(f"---Completed in {total_time:.3f} seconds")


if __name__ == "__main__":
    from core.cornershop.items import items_list

    fetch_data_to_gcs(items_list)
