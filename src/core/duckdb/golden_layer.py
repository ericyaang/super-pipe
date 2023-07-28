import os
from datetime import datetime

import duckdb
from dotenv import load_dotenv
from prefect import flow, task

from core.duckdb.silver_layer import (deduplication, load_parquet_from_bucket,
                                      save_parquet_to_bucket,
                                      setup_duckdb_connection)
from core.duckdb.utils import handle_path_date

# Load environment variables from .env file
load_dotenv()

# load env variables
ACCESS = os.getenv("GCS_ACCESS_KEY")
SECRET = os.getenv("GCS_SECRET")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BUCKET_PATH_SILVER = os.getenv("GCS_BUCKET_PATH_SILVER")
BUCKET_PATH_GOLD = os.getenv("GCS_BUCKET_PATH_GOLD")


@task
def create_golden_table(duckdb_connection: duckdb.DuckDBPyConnection):
    """
    Joins product_info, store_info, and transaction_info tables
    to create the golden layer.
    """
    try:
        duckdb_connection.sql(
            f"""
        CREATE TABLE gold_data AS
        SELECT 
            t.date, 
            p.aisle_name, 
            p.product_name, 
            p.brand, 
            p.package,
            p.search_term,
            t.price, 
            s.store_name, 
            s.store_city
        FROM transaction_info AS t
        LEFT JOIN product_info AS p
        ON t.product_id = p.product_id
        LEFT JOIN store_info AS s
        ON t.store_id = s.store_id
    """
        )
    except Exception as e:
        print(f"Failed to create golden table: {e}")
        raise


@flow(name="Generate-Golden-Data")
def generate_golden_data(date: str = "today"):
    # Set up your duckdb connection
    duckdb_conn = setup_duckdb_connection(ACCESS, SECRET)

    # Use the provided date
    DATETIME_UPLOADED = handle_path_date(date)

    # Timestamp for saved data
    HOUR_UPLOADED = datetime.now().strftime("%H%M%S")

    # Table paths for the selected date
    bucket_path_silver_product = (
        f"{BUCKET_NAME}/{BUCKET_PATH_SILVER}/{DATETIME_UPLOADED}/product_info_*.parquet"
    )
    bucket_path_silver_store = (
        f"{BUCKET_NAME}/{BUCKET_PATH_SILVER}/{DATETIME_UPLOADED}/store_info_*.parquet"
    )
    bucket_path_silver_transaction = f"{BUCKET_NAME}/{BUCKET_PATH_SILVER}/{DATETIME_UPLOADED}/transaction_info_*.parquet"

    # Load silver data into memory
    load_parquet_from_bucket(duckdb_conn, "product_info", bucket_path_silver_product)
    load_parquet_from_bucket(duckdb_conn, "store_info", bucket_path_silver_store)
    load_parquet_from_bucket(
        duckdb_conn, "transaction_info", bucket_path_silver_transaction
    )

    # Create the golden table
    create_golden_table(duckdb_conn)
    deduplication(duckdb_conn, "gold_data")

    # Save the golden table to a bucket
    bucket_path_golden = f"{BUCKET_NAME}/{BUCKET_PATH_GOLD}/{DATETIME_UPLOADED}/golden_table_{HOUR_UPLOADED}.parquet"
    save_parquet_to_bucket(duckdb_conn, "gold_data", bucket_path_golden)

    duckdb_conn.close()


if __name__ == "__main__":
    generate_golden_data()
