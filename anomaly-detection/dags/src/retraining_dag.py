from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from src.training.model_retrain import evaluate_model
from src.training.model_trainer import train_initial_model
from src.preprocessing.s3_loader import load_data_from_s3

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='memstream_retraining_dag',
    default_args=default_args,
    schedule_interval='@weekly',
    start_date=days_ago(1),
    catchup=False,
    description='Weekly retraining DAG for MemStream model using Spark'
) as dag:
    load_data = PythonOperator(
        task_id='load_data_from_s3',
        python_callable=load_data_from_s3,
        op_kwargs={
            'key': 'latest_access_log.xlsx'
        },
        provide_context=True,
    )

    check_performance = BranchPythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
        provide_context=True
    )

    retrain_task = PythonOperator(
        task_id='retrain_model',
        python_callable=train_initial_model,
        provide_context=True
    )

    skip_task = PythonOperator(
        task_id='skip_retraining',
        python_callable=lambda: print("Model is healthy, skipping retraining.")
    )

    load_data >> check_performance >> [retrain_task, skip_task]