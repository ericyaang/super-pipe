"""
python blocks.py -b $GITHUB_REF_NAME -r "$GITHUB_SERVER_URL/$GITHUB_REPOSITORY" \
-n ${{ inputs.block_name }} -i ${{ inputs.image_uri }} --region ${{ inputs.region }}
"""
import argparse
from prefect.filesystems import GitHub
from prefect_gcp.cloud_run import CloudRunJob
from prefect_gcp.credentials import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

REPO = "https://github.com/ericyaang/super-pipe"
parser = argparse.ArgumentParser()

parser.add_argument("-bn", "--bucket_name", default="cornershop-raw")
parser.add_argument("-bbn", "--bucket_block_name", default="default")

parser.add_argument("-b", "--branch", default="main")
parser.add_argument("-r", "--repo", default=REPO)

parser.add_argument("-n", "--block-name", default="default")
parser.add_argument("-i", "--image")
parser.add_argument("--region", default="southamerica-east1")

args = parser.parse_args()

bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load(args.block_name),
    bucket=args.bucket_name,
)
bucket_block.save(args.bucket_block_name, overwrite=True)

gh = GitHub(repository=args.repo, reference=args.branch)
gh.save(args.block_name, overwrite=True)

block = CloudRunJob(
    image=args.image,
    region=args.region,
    credentials=GcpCredentials.load(args.block_name),
    cpu=1,
    timeout=3600,
)
block.save(args.block_name, overwrite=True)


