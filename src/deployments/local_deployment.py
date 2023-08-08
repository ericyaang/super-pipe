import os

from prefect.deployments import Deployment
from prefect.infrastructure import Process
from prefect.infrastructure.container import DockerContainer
from prefect.server.schemas.schedules import CronSchedule

from flows.CornerShop import run_corner_shop_flow
from core.cornershop.items import items_list as products_list

deployment = Deployment.build_from_flow(
    flow=run_corner_shop_flow,
    name="Local-Deploy",
    parameters={"items": products_list},
    version=1,
    tags=["ETL-Local"],
    infrastructure=Process(working_dir=os.getcwd()),
    # Run the flow at 03:00 pm every Monday
    schedule=CronSchedule(cron="15 19 * * *", timezone="America/Sao_Paulo"),
    work_queue_name="default",
)

if __name__ == "__main__":
    deployment.apply()
