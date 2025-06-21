FROM apache/airflow:2.10.5

USER airflow

WORKDIR /opt/airflow/

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir \
    apache-airflow==2.10.5 \
    scikit-learn==1.6.1 \
    && pip install --no-cache-dir -r requirements.txt