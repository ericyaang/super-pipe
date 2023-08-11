from prefect_gcp.cloud_run import CloudRunJob
from prefect_gcp import GcpCredentials
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_NAME = os.getenv("GCP_PROJECT_NAME")
GCS_LOCATION = os.getenv("GCS_BUCKET_LOCATION")
credentials = GcpCredentials.load("default")

block = CloudRunJob(
    credentials=credentials,
    project=PROJECT_NAME,
    image="<YOUR-IMAGE-ADRESS>",
    region=GCS_LOCATION,
    cpu=1,
    timeout=3600,
)

block.save("cloud-run-infrastructure", overwrite=True)
