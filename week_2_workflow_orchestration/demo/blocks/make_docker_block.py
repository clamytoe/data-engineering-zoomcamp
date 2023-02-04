from prefect.infrastructure.docker import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="clamytoe/prefect:zoom",
    image_pull_policy="ALWAYS",
    auto_remove=True,
)

docker_block.save("dtc-de-zoom", overwrite=True)
