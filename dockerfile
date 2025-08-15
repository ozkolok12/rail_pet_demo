FROM apache/airflow:2.9.3

ARG AIRFLOW_UID=1000
USER root
RUN usermod -u ${AIRFLOW_UID} airflow
USER airflow