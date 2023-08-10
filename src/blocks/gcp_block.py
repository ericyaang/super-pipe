import json
import os

from dotenv import load_dotenv
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

load_dotenv()

GCP_CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

def load_service_account_info(service_account_file):
    # Check if service_account_file exists
    if not os.path.isfile(service_account_file):
        raise FileNotFoundError("Service account file not found")

    # Load the service account info from the file
    with open(service_account_file, "r") as file:
        service_account_info = json.load(file)

    return service_account_info


def save_credentials(service_account_info, block_name):
    # Create GCP credentials block
    credentials_block = GcpCredentials(service_account_info=service_account_info)

    # Save the credentials
    credentials_block.save(block_name, overwrite=True)

    return credentials_block


def save_bucket(credentials_block, bucket_name, block_name):
    # Create GCP bucket block
    bucket_block = GcsBucket(
        gcp_credentials=credentials_block,
        bucket=bucket_name,
    )

    # Save the bucket block
    bucket_block.save(block_name, overwrite=True)


if __name__ == "__main__":
    service_account_info = load_service_account_info(GCP_CREDENTIALS_PATH)
    
    credentials_block = save_credentials(service_account_info, "dafault")
    save_bucket(credentials_block, BUCKET_NAME, "default")
