import logging
import os
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts.test_import import test_list
from scripts.test_import import test_dic

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="test_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["test"],
)
def test_pipeline():
    @task(task_id="test_first_task")
    def test_first_task(*args, **kwargs):
        logging.info(test_list)
        logging.info(test_dic)
        logging.info("First test task successfully completed")

    @task(task_id="test_second_task")
    def test_second_task(*args, **kwargs):
        logging.info("Second test task successfully completed")

    first = test_first_task()
    test_second_task(first)


test_dag = test_pipeline()
