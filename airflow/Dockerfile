# Airflow for the fire DWH: the official image plus the extraction
# dependencies, so DAG tasks can run data-extraction-scripts/ directly.
FROM apache/airflow:2.10.5-python3.12

COPY requirements.txt /tmp/requirements.txt

# Pinning apache-airflow to the image's own version stops pip from upgrading
# or downgrading Airflow while it resolves the extraction packages.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
