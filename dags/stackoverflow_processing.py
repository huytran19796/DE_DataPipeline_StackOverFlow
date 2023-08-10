from airflow import DAG
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.operators.bash import BashOperator
from utility import google_drive_downloader as gdd
import os
import pandas as pd
import pymongo
import json
from datetime import datetime

QUESTIONS_FILE = '/tmp/Questions.csv'
ANSWERS_FILE = '/tmp/Answers.csv'
TEST_FILE = '/tmp/Test.csv'

def _check_data():
    if os.path.isfile(QUESTIONS_FILE) and os.path.isfile(QUESTIONS_FILE):
        return "spark_process"
    else:
        return "clear_data"

def _clear_data():
    if os.path.isfile(QUESTIONS_FILE):
        os.remove(QUESTIONS_FILE)
    if os.path.isfile(ANSWERS_FILE):
        os.remove(ANSWERS_FILE)

def _download_question():
    gdd.GoogleDriveDownloader.download_file_from_google_drive(file_id='1ZrrwBAH_cwWw25uQo0Aa7pqdO2BKnCTr', dest_path=QUESTIONS_FILE)

def _import_question():
    connection = pymongo.MongoClient("mongodb://172.29.32.1:27017",connect=False, authSource="admin")
    database = connection['stackOverFlow_as17']
    collection = database['questions']
    data = pd.read_csv(QUESTIONS_FILE)
    data_json = json.loads(data.to_json(orient='records'))
    collection.insert_many(data_json)

def _download_answers():
    gdd.GoogleDriveDownloader.download_file_from_google_drive(file_id='1QEoxx632_eSKsvgRRJeQZD9sO27I9v-u', dest_path=ANSWERS_FILE)

def _import_answers():
    connection = pymongo.MongoClient("mongodb://172.29.32.1:27017",connect=False, authSource="admin")
    database = connection['stackOverFlow_as17']
    collection = database['answers']
    data = pd.read_csv(ANSWERS_FILE)
    data_json = json.loads(data.to_json(orient='records'))
    collection.insert_many(data_json)

with DAG('stackoverflow_processing', start_date=datetime(2023, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:
    
    # Task 1: DummyOperator làm đánh dấu thành công
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    check_data = BranchPythonOperator(
        task_id = 'check_data',
        python_callable=_check_data
    )

    clear_data = PythonOperator(
        task_id = 'clear_data',
        python_callable=_clear_data
    )

    download_question = PythonOperator(
        task_id = 'download_question',
        python_callable= _download_question
    )

    import_question = PythonOperator(
        task_id = 'import_question',
        python_callable=_import_question
    )

    download_answers = PythonOperator(
        task_id = 'download_answers',
        python_callable= _download_answers
    )

    import_answers = PythonOperator(
        task_id = 'import_answers',
        python_callable=_import_answers
    )

    spark_process = SparkSubmitOperator(
    task_id='spark_process',
    conn_id="spark_default",  # Kết nối đã được cấu hình trong Airflow
    application="dags/utility/spark_process.py",
    trigger_rule='all_success'
    )

    start >> check_data >> [clear_data, spark_process]
    clear_data >> download_question >> import_question >> spark_process >> end
    clear_data >> download_answers >> import_answers >> spark_process >> end