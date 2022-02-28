import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import storage

from unzip_file_to_csv import unzip_file_get_df
from feature_engineering import get_feat_eng_df
from clean_reviews import get_clean_review_df

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")
PATH_TO_CONTAINER_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def get_download_url_list():
    download_url_file = open("/opt/airflow/dags/download_url.txt", "r")
    logging.info("reading download_url.txt file...")
    download_url_file_content = download_url_file.read()
    download_url_file_list = download_url_file_content.split("\n")
    logging.info(f"list loadeded...with {len(download_url_file_list)} urls...")
    download_url_file.close()
    logging.info("file closed...")
    return download_url_file_list


def get_url_filename_list():
    download_url_file_list = get_download_url_list()
    url_filename_list = []
    for url in download_url_file_list:
        filename = url.rsplit("/", 1)[1]
        url_filename_list.append(filename)
    return url_filename_list


def upload_raw_to_gcs():
    url_filename_list = get_url_filename_list()
    for filename in url_filename_list:
        bucket = GCP_GCS_BUCKET
        object_name = f"raw_amazon_data/{filename[8:-10]}.csv"
        local_file = f"{PATH_TO_CONTAINER_HOME}/{filename}.csv"

        client = storage.Client()
        bucket = client.bucket(bucket)

        logging.info("Uploading {object_name}...")
        blob = bucket.blob(object_name)
        blob.upload_from_filename(f"{local_file}")
        logging.info("Upload {object_name} completed...")


def upload_clean_to_gcs():
    # bucket = GCP_GCS_BUCKET
    url_filename_list = get_url_filename_list()
    for filename in url_filename_list:
        bucket = GCP_GCS_BUCKET
        object_name = f"clean_amazon_data/{filename[8:-10]}.csv"
        local_file = f"{PATH_TO_CONTAINER_HOME}/{filename}.csv"

        client = storage.Client()
        bucket = client.bucket(bucket)

        logging.info("Uploading {object_name}...")
        blob = bucket.blob(object_name)
        blob.upload_from_filename(f"{local_file}")
        logging.info("Upload {object_name} completed...")


# def unzip_file_to_csv_task():
#     url_filename_list = get_url_filename_list()
#     for filename in url_filename_list:
#         local_file = f"{PATH_TO_CONTAINER_HOME}/{filename}"
#         logging.info(f"unzipping file for {filename}")
#         df = unzip_file_to_csv(local_file)
#         logging.info(f"{local_file}...df shape...{df.shape}")
#         logging.info(f"Saving {local_file} to .csv format...")
#         df.to_csv(f"{PATH_TO_CONTAINER_HOME}/{filename}.csv", index=False)


def get_unzip_file_to_csv():
    url_filename_list = get_url_filename_list()
    for filename in url_filename_list:
        local_file = f"{PATH_TO_CONTAINER_HOME}/{filename}"
        logging.info(f"unzipping file for {filename}")
        df = unzip_file_get_df(local_file)
        logging.info(f"{local_file}...df shape...{df.shape}")
        logging.info(f"Saving {local_file} to .csv format...")
        df.to_csv(f"{PATH_TO_CONTAINER_HOME}/{filename}.csv", index=False)


def run_feature_engineering():
    url_filename_list = get_url_filename_list()
    for filename in url_filename_list:
        local_file = f"{PATH_TO_CONTAINER_HOME}/{filename}.csv"
        df = get_feat_eng_df(local_file)
        df.to_csv(f"{PATH_TO_CONTAINER_HOME}/{filename}.csv", index=False)


def run_clean_review():
    url_filename_list = get_url_filename_list()
    for filename in url_filename_list:
        local_file = f"{PATH_TO_CONTAINER_HOME}/{filename}.csv"
        df = get_clean_review_df(local_file)
        df.to_csv(f"{PATH_TO_CONTAINER_HOME}/{filename}.csv", index=False)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 2, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="ingest_amazon_data_to_GCS_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=2,
    tags=["amazon_data"],
) as dag:

    download_url_file_list = get_download_url_list()
    download_url_file_list_str = " ".join(download_url_file_list)
    download_data_task = BashOperator(
        task_id="download_data_task",
        bash_command=f"""
        cd {PATH_TO_CONTAINER_HOME}
        for value in {download_url_file_list_str}
        do
            echo $value
            curl -OJ $value
        done
        """,
    )

    # unzip_file_to_csv_task = PythonOperator(
    #     task_id="unzip_file_to_csv_task",
    #     python_callable=unzip_file_to_csv_task,
    # )

    # unzip_file_to_json_task = BashOperator(
    #     task_id="unzip_file_to_json_task",
    #     bash_command=f"""
    #     cd {PATH_TO_CONTAINER_HOME}
    #     for value in {url_filename_list_str}
    #     do
    #         gunzip --keep $value
    #     done
    #     """,
    # )

    unzip_file_to_csv_task = PythonOperator(
        task_id="unzip_file_to_csv_task",
        python_callable=get_unzip_file_to_csv,
    )

    feature_engineer_task = PythonOperator(
        task_id="feature_engineer_task",
        python_callable=run_feature_engineering,
    )

    clean_review_task = PythonOperator(
        task_id="clean_review_task",
        python_callable=run_clean_review,
    )

    ingest_raw_data_to_GCS_task = PythonOperator(
        task_id="ingest_raw_data_to_GCS_task",
        python_callable=upload_raw_to_gcs,
    )

    ingest_clean_data_to_GCS_task = PythonOperator(
        task_id="ingest_clean_data_to_GCS_task",
        python_callable=upload_clean_to_gcs,
    )

    url_filename_list = get_url_filename_list()
    url_filename_list_str = " ".join(url_filename_list)
    remove_container_file_task = BashOperator(
        task_id="remove_container_file_task",
        bash_command=f"""
        cd {PATH_TO_CONTAINER_HOME}
        for value in {url_filename_list_str}
        do  
            echo $value
            rm $value $value.csv
        done
        """,
    )

    (
        download_data_task
        >> unzip_file_to_csv_task
        >> feature_engineer_task
        >> clean_review_task
        >> ingest_clean_data_to_GCS_task
        >> remove_container_file_task
    )

    (download_data_task >> unzip_file_to_csv_task >> ingest_raw_data_to_GCS_task)
