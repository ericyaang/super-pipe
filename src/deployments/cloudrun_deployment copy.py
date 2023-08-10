from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from flows.CornerShop import run_corner_shop_flow


from prefect_gcp.cloud_run import CloudRunJob
from prefect.filesystems import GitHub

github_block = GitHub.load("super-pipe")
cloud_run_job_block = CloudRunJob.load("cloud-run-infrastructure")

deployment = Deployment.build_from_flow(
    flow=run_corner_shop_flow,
    name="corner-cloudrun-flows",
    parameters={"items": ["haribo"]},
    version=1,
    tags=["TESTE-1"],
    storage=github_block,
    infrastructure=cloud_run_job_block,
    schedule=CronSchedule(cron="10 12 * * *", timezone="America/Sao_Paulo"),
    work_queue_name="default",
    work_pool_name="default-agent-pool",
    path="src"
)

if __name__ == "__main__":
    deployment.apply()

# After that run: prefect agent start --pool default-agent-pool --work-queue default
