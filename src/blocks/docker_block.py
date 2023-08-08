from prefect.infrastructure.container import DockerContainer, ImagePullPolicy


def create_docker_container(block_name, docker_name, image_name):
    block = DockerContainer(
        name=docker_name,
        image=image_name,
        # We want to always use our local image, so we NEVER pull it
        image_pull_policy=ImagePullPolicy.NEVER,
    )
    uuid = block.save(block_name, overwrite=True)
    print(uuid)


if __name__ == "__main__":
    create_docker_container(
        block_name="conershop-docker",
        docker_name="corner-test",
        image_name="prefect-docker",
    )
