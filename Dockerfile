FROM apache/airflow:2.8.0-python3.11

# Install pipeline dependencies as the airflow user (avoids permission warnings)
RUN pip install --no-cache-dir \
    "numpy<2" \
    dbt-core==1.7.0 \
    dbt-duckdb==1.7.0 \
    duckdb==1.5.3 \
    pandas==2.1.0 \
    openpyxl==3.1.2 \
    "requests>=2.31.0" \
    great-expectations==0.18.0
