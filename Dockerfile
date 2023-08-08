FROM prefecthq/prefect:2-python3.10 AS base

ENV PATH="/root/.local/bin:${PATH}"
ENV PYTHONUNBUFFERED True

# atualizar repositórios
RUN apt-get update -qq && \
    apt-get -qq install \
    curl

FROM base AS python-depedencies 

# Instalar Poetry
WORKDIR /opt/prefect

RUN curl -sSL https://install.python-poetry.org | python3 -

COPY pyproject.toml .
COPY poetry.lock .

RUN poetry install -q -n --no-root --without dev --no-cache --no-interaction
