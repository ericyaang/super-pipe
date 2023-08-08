# Local Dev Setup

### Setup
- run `make all`
- `poetry init`
- Add necessary packages via `poetry add`

#### pre-commit hooks
- `poetry add --dev pre-commit`
- Create a pre-commit config file: `.pre-commit-config.yaml`
- Install the pre-commit hooks: `poetry run pre-commit install`
- Run pre-commit: `poetry run pre-commit run --all-files`


### Google Cloud Plataform
- Crie a sua conta de serviço com as permissões necessárias
- Crie a sua conta Prefect Cloud
- Logue a sua conta no terminal `prefect cloud login`
- Substitua as informações do `.envExample` e renomei para `.env`
- Registre os blocks executando src/utils/create_blocks.py

Saiba mais em https://prefecthq.github.io/prefect-gcp/#getting-started


### Flow

- A pipeline dura em torno de 17 minutos para ser finalizada


### Docker
- Create Dockerfile
- Build image: `docker build -t corner-etl .`
- Push image: `docker image push corner-etl`