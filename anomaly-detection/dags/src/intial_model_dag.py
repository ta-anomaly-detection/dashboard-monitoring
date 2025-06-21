from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from src.preprocessing.s3_loader import load_data_from_s3
from src.training.model_trainer import train_initial_model

with DAG(
    dag_id='initial_model_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@once',
    catchup=False
) as dag:

    load_data_task = PythonOperator(
        task_id='load_data_from_s3',
        python_callable=load_data_from_s3,
        provide_context=True
    )

    preprocess_data_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=train_initial_model,
        provide_context=True
    )

    trigger_notify = TriggerDagRunOperator(
        task_id="trigger_reload_model_dag",
        trigger_dag_id="model_deployment",
    )

    load_data_task >> preprocess_data_task >> trigger_notify
