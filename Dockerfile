FROM prefecthq/prefect:2-python3.10

ENV PYTHONPATH "${PYTHONPATH}:/opt/prefect/"

# Accept build arguments
ARG PREFECT_API_KEY
ARG PREFECT_API_URL

# Set the environment variables within the container
ENV PREFECT_API_URL=$PREFECT_API_URL
ENV PREFECT_API_KEY=$PREFECT_API_KEY

ENV PYTHONUNBUFFERED True

# Set a working directory
WORKDIR /opt/prefect

COPY requirements.txt .
COPY setup.py .
COPY src/core core

RUN pip install --upgrade pip setuptools --no-cache-dir
RUN pip --no-cache-dir install -r requirements.txt
RUN pip --no-cache-dir install .

COPY src/flows /opt/prefect/flows

ENTRYPOINT ["prefect", "agent", "start", "-q", "default"]