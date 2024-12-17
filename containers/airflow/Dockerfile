FROM apache/airflow:2.9.2-python3.11

USER root

RUN apt-get update \
    && apt-get install -y \
    curl \
    libpq-dev \
    gcc \
    wget \
    procps \
    default-jre \
    && apt-get clean  \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o /opt/airflow/minio-binaries/mc && \
  chmod +x /opt/airflow/minio-binaries/mc

ENV PATH=$PATH:/opt/airflow/minio-binaries/

USER airflow
COPY --chown=airflow:airflow containers/airflow/config.json /home/airflow/.mc/config.json
COPY --chown=airflow:airflow requirements.txt ./requirements.txt
RUN mc config host list && \
  pip install -r requirements.txt
