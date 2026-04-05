FROM apache/airflow:3.0.6

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r /requirements.txt
