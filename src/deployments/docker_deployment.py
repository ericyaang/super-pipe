from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule

from flows.CornerShop import run_corner_shop_flow

docker_container_block = DockerContainer.load("conershop-docker")

deployment = Deployment.build_from_flow(
    flow=run_corner_shop_flow,
    name="corner-teste-docker",
    parameters={"items": ["haribo"]},
    version=1,
    tags=["DOCKER-TESTE"],
    infrastructure=docker_container_block,
    schedule=CronSchedule(cron="30 21 * * *", timezone="America/Sao_Paulo"),
    work_queue_name="default",
    work_pool_name="default-agent-pool",
)

if __name__ == "__main__":
    deployment.apply()

# After that run: prefect agent start --pool default-agent-pool --work-queue default
