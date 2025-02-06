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

USER airflow
COPY --chown=airflow:airflow requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
