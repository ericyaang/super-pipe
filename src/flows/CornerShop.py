from time import perf_counter
from core.cornershop.main import fetch_corner_shop_data
from core.google_cloud_storage.main import load_to_parquet_and_upload_to_gcs
from prefect import flow, get_run_logger

@flow(name="CornerShop Main Flow")
def CornerShop():
    logger = get_run_logger()

    logger.info("---Starting CornerShop Main Flow---")
    start_time = perf_counter()

    data = fetch_corner_shop_data()

    logger.info("Loading data into Parquet and then uploading to Google Cloud Storage")
    load_to_parquet_and_upload_to_gcs(data)
    end_time = perf_counter()
    total_time = end_time - start_time
    logger.info(f"---Completed Cornershop Main Flow in {total_time:.3f} seconds")

if __name__ == "__main__":
    CornerShop()