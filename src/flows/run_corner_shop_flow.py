from time import perf_counter

from prefect import flow, get_run_logger


from core.cornershop.main import fetch_corner_shop_data
from core.duckdb.golden_layer import generate_golden_data
from core.duckdb.silver_layer import generate_silver_data
from core.google_bigquery.main import gcs_to_bigquery
from core.google_cloud_storage.main import load_to_parquet_and_upload_to_gcs


@flow(name="CornerShop Main Flow", log_prints=True)
def run_corner_shop_flow(items: list):
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

    logger.info("---Starting CornerShop Silver Layer Flow---")
    generate_silver_data()

    logger.info("---Starting CornerShop Golden Layer Flow---")
    generate_golden_data()

    logger.info("---Loading from GCS to BigQuery---")
    gcs_to_bigquery()

    end_time = perf_counter()
    total_time = end_time - start_time
    logger.info(f"---Completed Cornershop Main Flow in {total_time:.3f} seconds")


if __name__ == "__main__":
    from core.cornershop.items import items_list

    run_corner_shop_flow(items_list)
