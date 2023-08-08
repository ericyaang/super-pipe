from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule
from typing import Final
from dotenv import load_dotenv
from prefect.deployments import Deployment
from prefect.infrastructure.container import (
    DockerContainer,
    DockerRegistry,
    ImagePullPolicy,
)

from flows.CornerShop import run_corner_shop_flow
import logging as logger
import os
from dotenv import load_dotenv

logger.info("Lendo configurações a partir de variáveis de ambiente...")

load_dotenv()


# Configurações do registro e da imagem Docker
DOCKER_IMAGEM = os.getenv("DOCKER_IMAGEM")
DOCKER_REGISTRO_SENHA = os.getenv("DOCKER_REGISTRO_SENHA")
DOCKER_REGISTRO_URL = os.getenv("DOCKER_REGISTRO_URL")
DOCKER_REGISTRO_USUARIO = os.getenv("DOCKER_REGISTRO_USUARIO", "")

# Configurações do Prefect
PREFECT_API_URL = os.getenv("PREFECT_API_URL")
PREFECT_API_KEY = os.getenv("PREFECT_API_KEY")

logger.info("Configurando bloco de infraestrutura com Docker...")
bloco_docker_registro = DockerRegistry(
    username=DOCKER_REGISTRO_USUARIO,
    password=DOCKER_REGISTRO_SENHA,
    registry_url=DOCKER_REGISTRO_URL,
    reauth=True,
)
bloco_docker_registro.save(
    name="corner-docker-registry",
    overwrite=True,
)

logger.info("Configurando bloco de infraestrutura com Docker...")
bloco_docker_container = DockerContainer(
    auto_remove=True,
    # command=["./entrypoint.sh"],
    env={"PREFECT_API_URL": PREFECT_API_URL, "PREFECT_API_KEY": PREFECT_API_KEY},
    image=DOCKER_IMAGEM,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    image_registry=bloco_docker_registro,
    name="corner-docker-container",
    stream_output=True,
)
bloco_docker_container.save(
    name="docker-container-impulsoetl",
    overwrite=True,
)

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=run_corner_shop_flow,
        name="corner-teste-docker",
        # output=None,
        skip_upload=True,
        apply=False,
        load_existing=True,
        parameters={"items": ["haribo"]},
        tags=["DOCKER-TESTE"],
        infrastructure=bloco_docker_container,
        schedule=CronSchedule(cron="30 21 * * *", timezone="America/Sao_Paulo"),
        work_queue_name="default",
        work_pool_name="default-agent-pool",
        path="",
        version=1,
    )
    deployment.apply()
