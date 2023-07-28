ENV_DIR := .venv

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

