from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from dotenv import load_dotenv
import json 
import os

# Load environment variables from .env file
load_dotenv()

# Load env variables
service_account_file = os.getenv("GCP_CREDENTIALS_PATH")
bucket_name = os.getenv("GCS_BUCKET_NAME")

# Load the service account info from the file
with open(service_account_file, 'r') as file:
    service_account_info = json.load(file)


# create gcp credentials block
credentials_block = GcpCredentials(
    service_account_info=service_account_info
)

credentials_block.save("gcp-credentials", overwrite=True)


# create gcp bucket block
bucket_block = GcsBucket(
    gcp_credentials=credentials_block,
    bucket=bucket_name,
)
bucket_block.save("gcp-bucket-corner", overwrite=True)


