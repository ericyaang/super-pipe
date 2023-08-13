import os

from prefect.deployments import Deployment
from prefect.infrastructure import Process
from prefect.server.schemas.schedules import CronSchedule

from flows.run_corner_shop_flow import run_corner_shop_flow

deployment = Deployment.build_from_flow(
    flow=run_corner_shop_flow,
    name="Cornershop-Cloud",
    # parameters={'kwargs': {**kwargs}},
    version=1,
    tags=["ETL-Teste"],
    infrastructure=Process(working_dir=os.getcwd()),
    # Run the flow at 03:00 pm every Monday
    schedule=CronSchedule(cron="48 21 * * *", timezone="America/Sao_Paulo"),
    work_queue_name="default",
)

if __name__ == "__main__":
    deployment.apply()
