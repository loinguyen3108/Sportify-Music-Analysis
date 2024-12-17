FROM bitnami/spark:3.5.3
USER root
RUN install_packages wget build-essential zlib1g-dev curl
USER 1001
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt