import os
from datetime import datetime

import duckdb
from dotenv import load_dotenv
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
gcs_bucket_block = GcsBucket.load(
    "default"
)

# Load environment variables from .env file
load_dotenv()

# load env variables
ACCESS = os.getenv("GCS_ACCESS_KEY")
SECRET = os.getenv("GCS_SECRET")
BUCKET_NAME = gcs_bucket_block.bucket
#BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BUCKET_PATH_BRONZE = os.getenv("GCS_BUCKET_PATH_BRONZE")
BUCKET_PATH_SILVER = os.getenv("GCS_BUCKET_PATH_SILVER")
BUCKET_PATH_GOLD = os.getenv("GCS_BUCKET_PATH_GOLD")


@task
def setup_duckdb_connection(
    ACCESS, SECRET, db_path: str = ":memory:", read_only: str = False
):
    """
    Sets up a duckdb connection and configures it for S3 access.
    """
    try:
        duckdb_connection = duckdb.connect(database=db_path, read_only=read_only)
        duckdb_connection.sql("INSTALL httpfs")
        duckdb_connection.sql("LOAD httpfs")
        duckdb_connection.sql(f"SET s3_access_key_id='{ACCESS}'")
        duckdb_connection.sql(f"SET s3_secret_access_key='{SECRET}'")
        duckdb_connection.sql("SET s3_endpoint='storage.googleapis.com'")
        return duckdb_connection
    except Exception as e:
        print(f"Failed to setup DuckDB connection: {e}")
        raise


@task
def load_parquet_from_bucket(
    duckdb_connection: duckdb.DuckDBPyConnection, table_name: str, bucket_path: str
):
    """
    Loads parquet data from S3 and creates a table in DuckDB
    """
    try:
        duckdb_connection.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM 's3://{bucket_path}'"
        )
    except Exception as e:
        print(f"Failed to load parquet from bucket: {e}")
        raise


@task
def save_parquet_to_bucket(
    duckdb_connection: duckdb.DuckDBPyConnection, table_name: str, bucket_path: str
):
    """
    Saves a table from DuckDB as a parquet file in S3
    """
    try:
        duckdb_connection.sql(
            f"COPY {table_name} TO 's3://{bucket_path}' (FORMAT PARQUET);"
        )
    except Exception as e:
        print(f"Failed to save parquet to bucket: {e}")
        raise


@task
def deduplication(duckdb_connection: duckdb.DuckDBPyConnection, table_name: str):
    """
    Deduplicates rows in a table
    """
    try:
        duckdb_connection.sql(
            f"CREATE TABLE {table_name}_dedup AS SELECT DISTINCT * FROM {table_name}"
        )
        duckdb_connection.sql(f"DROP TABLE {table_name}")
        duckdb_connection.sql(f"ALTER TABLE {table_name}_dedup RENAME TO {table_name}")
    except Exception as e:
        print(f"Failed to deduplicate table: {e}")
        raise


@task
def handle_missing_values(
    duckdb_connection, table_name: str, columns: list, default_value="None"
):
    """
    Handle missing values in specific columns of a table
    """
    for column in columns:
        try:
            # Fill in missing values in the specified column with a default value
            duckdb_connection.sql(
                f"""
            UPDATE {table_name}
            SET {column} = COALESCE({column}, '{default_value}');
            """
            )
        except Exception as e:
            print(f"Failed to handle missing values in column {column}: {e}")
            raise


@task
def format_date_column(duckdb_connection: duckdb.DuckDBPyConnection, table_name: str):
    """
    Formats the date column in a table
    """
    try:
        duckdb_connection.sql(
            f"""
        ALTER TABLE {table_name}
        ALTER date SET DATA TYPE TIMESTAMP  
        USING strptime("date", '%d-%m-%Y');
        """
        )
    except Exception as e:
        print(f"Failed to format date column: {e}")
        raise


# Creating the product_info table
@task
def create_product_info_table(
    duckdb_connection: duckdb.DuckDBPyConnection, table_name: str
):
    return duckdb_connection.sql(
        f"""
        CREATE TABLE product_info AS
        SELECT DISTINCT 
            hash(aisle_name || product_name || brand || package || search_term) AS product_id, 
            aisle_name, 
            product_name, 
            brand, 
            package,
            search_term
        FROM {table_name}
    """
    )


# Creating the store_info table
@task
def create_store_info_table(
    duckdb_connection: duckdb.DuckDBPyConnection, table_name: str
):
    return duckdb_connection.sql(
        f"""
    CREATE TABLE store_info AS
    SELECT DISTINCT 
        hash(store_name || store_city) AS store_id, 
        store_name, 
        store_city
    FROM {table_name}
"""
    )


# Creating the transaction_info table (FACT TABLE)
@task
def create_transaction_info_table(
    duckdb_connection: duckdb.DuckDBPyConnection, table_name: str
):
    return duckdb_connection.sql(
        f"""
        CREATE TABLE transaction_info AS
        SELECT 
            date, 
            price, 
            hash(aisle_name || product_name || brand || package || search_term) AS product_id,
            hash(store_name || store_city) AS store_id
        FROM {table_name}
    """
    )


@flow(name="Generate-Silver-Data")
def generate_silver_data(table_name: str = "my_table"):
    # Timestamp for saved data
    HOUR_UPLOADED = datetime.now().strftime("%H%M%S")

    # Use the provided date
    DATETIME_TRANSFORMED = datetime.now().strftime("year=%Y/month=%m/day=%d")

    bucket_path_bronze = f"{BUCKET_NAME}/{BUCKET_PATH_BRONZE}"
    bucket_path_silver = f"{BUCKET_NAME}/{BUCKET_PATH_SILVER}"

    # Establish a connection to DuckDB
    duckdb_conn = setup_duckdb_connection(ACCESS, SECRET)

    # Load data from the bronze layer
    load_parquet_from_bucket(
        duckdb_conn,
        table_name,
        f"{bucket_path_bronze}/{DATETIME_TRANSFORMED}/*/*/*.parquet",
    )

    # Clean data <- Could more effienct using a class but Prefect does not work well with OOP
    deduplication(duckdb_conn, table_name)
    handle_missing_values(duckdb_conn, table_name, ["brand", "package"], "None")
    format_date_column(duckdb_conn, table_name)
    deduplication(duckdb_conn, table_name)

    # Normalize data creating product_info, store_info and transaction_info tables
    create_product_info_table(duckdb_conn, table_name)
    create_store_info_table(duckdb_conn, table_name)
    create_transaction_info_table(duckdb_conn, table_name)

    # Save tables to silver layer
    save_parquet_to_bucket(
        duckdb_conn,
        "product_info",
        f"{bucket_path_silver}/{DATETIME_TRANSFORMED}/product_info_{HOUR_UPLOADED}.parquet",
    )
    save_parquet_to_bucket(
        duckdb_conn,
        "store_info",
        f"{bucket_path_silver}/{DATETIME_TRANSFORMED}/store_info_{HOUR_UPLOADED}.parquet",
    )
    save_parquet_to_bucket(
        duckdb_conn,
        "transaction_info",
        f"{bucket_path_silver}/{DATETIME_TRANSFORMED}/transaction_info_{HOUR_UPLOADED}.parquet",
    )

    duckdb_conn.close()
    return DATETIME_TRANSFORMED


if __name__ == "__main__":
    generate_silver_data()
