from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 7),
    'retries': 1,
}

with DAG(
    'youtube_data_pipeline',
    default_args=default_args,
    description='A pipeline to extract and transform YouTube data',
    schedule='@daily',  # corrected argument
    catchup=False,
) as dag:

    extract_task = BashOperator(
        task_id='extract_youtube_data',
        bash_command='/home/main/youtube/env/bin/python /home/main/youtube/extract.py',
    )

    transform_task = BashOperator(
        task_id='transform_and_load',
        bash_command='/home/main/youtube/env/bin/python /home/main/youtube/transform.py',
    )

    extract_task >> transform_task

