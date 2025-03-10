FROM apache/airflow:2.6.3-python3.10

USER root

# Install system dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         default-libmysqlclient-dev \
         libpq-dev \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies with specific versions to avoid conflicts
RUN pip install --no-cache-dir \
    pandas==1.5.3 \
    numpy==1.24.3 \
    pyarrow==12.0.0 \
    google-cloud-storage==2.9.0 \
    google-cloud-bigquery==3.11.0 \
    dbt-core==1.5.0 \
    dbt-bigquery==1.5.0 \
    great-expectations==0.16.13 \
    pytest==7.3.1 \
    pytest-cov==4.1.0 \
    python-dotenv==1.0.0 \
    pyyaml==6.0 \
    setuptools==68.0.0

# Install Airflow providers with specific versions
RUN pip install --no-cache-dir \
    apache-airflow-providers-google==10.0.0 \
    apache-airflow-providers-dbt-cloud==3.2.0 \
    sqlalchemy==1.4.46 \
    pendulum==2.1.2

# Set environment variables
ENV PYTHONPATH=/opt/airflow
ENV AIRFLOW_HOME=/opt/airflow

# Create directories
RUN mkdir -p /opt/airflow/bronze /opt/airflow/silver /opt/airflow/gold /opt/airflow/tests

WORKDIR /opt/airflow 