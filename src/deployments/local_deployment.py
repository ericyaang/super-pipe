from prefect.deployments import Deployment
from prefect.infrastructure import Process
from prefect.server.schemas.schedules import CronSchedule
from flows.run_corner_shop_flow import run_corner_shop_flow

deployment = Deployment.build_from_flow(
    flow=run_corner_shop_flow,
    name="Local-Deploy",
    parameters={"items": ["haribo"]},
    version=1,
    tags=["ETL-Local"],
    infrastructure=Process(),
    schedule=CronSchedule(cron="41 10 * * *", timezone="America/Sao_Paulo"),
    work_queue_name="default",
    work_pool_name="default-agent-pool",
)

if __name__ == "__main__":
    deployment.apply()
