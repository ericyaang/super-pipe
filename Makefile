env:
	@echo "Creating virtual environment"
	python -m venv .venv

activate:
	powershell -noexit -executionpolicy bypass .venv/Scripts/activate.ps1

install:
	@echo "Installing pip, setuptools and poetry"
	python -m pip install --upgrade pip setuptools poetry --no-cache-dir

prefect:
	@echo "Installing prefect 2"
	pip install -U prefect

packages:
	@echo "Installing necessary packages"
	pip install python-dotenv requests google-cloud-storage

all: env activate install prefect packages


clean:
	if exist .\\.git\\hooks ( rmdir .\\.git\\hooks /q /s )
	if exist .\\.venv\\ ( rmdir .\\.venv /q /s )
	if exist poetry.lock ( del poetry.lock /q /s )


poetry-requirements:
	poetry export -f requirements.txt --output requirements.txt --without-hashes


### Prefect pipeline

# Makefiles provide a handy way to codify commands. Instead of having to copy
# paste commands from a README, you can  execute them as "make <command>": for
# example "make all" or "make run". If your system doesn't include make, or if
# you just want to avoid it, you can always run the commands directly.

# .PHONY forces make to run commands even if files haven't changed.
.PHONY: build-image create-blocks deploy run all

# First, we build an image with our code and dependencies.
build-image:
	docker build -t corner-etl/teste:latest .

# Second, we create the infrastructure block that our deployment will use.
create-blocks:
	python blocks/docker_container.py

# Third, we create a deployment of our hello flow.
deploy:
	python deployments/docker_deployment.py

# Finally, we create a flow run and then start an agent to process it.
run:
	prefect agent start -q default

# Run "make all" to see the end to end process.
all: build-image create-blocks deploy run