from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from fetch_reddit_data import fetch_reddit_data
from preprocess_data import preprocess_data
from sentiment_analysis import perform_sentiment_analysis
from save_to_s3 import save_to_s3

# DAG definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    "reddit_sentiment_analysis",
    default_args=default_args,
    description="A Reddit sentiment analysis pipeline",
    schedule_interval="@daily",
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_reddit_data", python_callable=fetch_reddit_data
    )

    preprocess_task = PythonOperator(
        task_id="preprocess_data", python_callable=preprocess_data
    )

    sentiment_task = PythonOperator(
        task_id="sentiment_analysis", python_callable=perform_sentiment_analysis
    )

    save_task = PythonOperator(task_id="save_to_s3", python_callable=save_to_s3)

    fetch_task >> preprocess_task >> sentiment_task >> save_task
