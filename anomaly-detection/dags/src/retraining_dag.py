from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from src.training.model_retrain import check_model_performance

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
    check_performance = BranchPythonOperator(
        task_id='check_model_performance',
        python_callable=check_model_performance,
        provide_context=True
    )

    retrain_task = PythonOperator(
        task_id='retrain_model',
        python_callable=lambda: print("Retraining start.")
    )

    skip_task = PythonOperator(
        task_id='skip_retraining',
        python_callable=lambda: print("Model is healthy, skipping retraining.")
    )

    trigger_notify = TriggerDagRunOperator(
        task_id="trigger_reload_model_dag",
        trigger_dag_id="model_deployment",
    )


    check_performance >> [retrain_task >> trigger_notify, skip_task]