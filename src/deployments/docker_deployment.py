from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule

from flows.CornerShop import run_corner_shop_flow

from prefect.infrastructure.container import DockerContainer, ImagePullPolicy
import os
from dotenv import load_dotenv
load_dotenv()

from prefect.filesystems import GitHub

github_block = GitHub.load('corner-src')

docker_container_block = DockerContainer(
    name="docker-container",
    image="", # insert your image here
    auto_remove=True,
    # We want to always use our local image, so we NEVER pull it
    image_pull_policy=ImagePullPolicy.NEVER,
)

docker_container_block.save("etl", overwrite=True)

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
