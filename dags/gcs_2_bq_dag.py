# import os
# import logging

# from airflow import DAG
# from airflow.utils.dates import days_ago

# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
# from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


# PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET = os.environ.get("GCP_GCS_BUCKET")

# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'amazon_data_lake1')
# # BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_dbt')

# default_args = {
#     "owner": "airflow",
#     "start_date": days_ago(1),
#     "depends_on_past": False,
#     "retries": 1,
# }

# # NOTE: DAG declaration - using a Context Manager (an implicit way)
# with DAG(
#     dag_id="gcs_2_bq_dag",
#     schedule_interval="@daily",
#     default_args=default_args,
#     catchup=False,
#     max_active_runs=1,
#     tags=['amazon-data'],
# ) as dag:

#     # #to organise file structure
#     # gcs_2_gcs_task = GCSToGCSOperator(
#     #     task_id='gcs_2_gcs_task',
#     #     source_bucket=BUCKET,
#     #     # source_object="raw/yellow_tripdata*.parquet",
#     #     source_object="clean_amazon_data_zoomcamp/*.csv",
#     #     destination_bucket=BUCKET,
#     #     # destination_object="clean/clean_amazon_data",
#     #     move_object=False
#     # )


#     gcs_2_bq_clean_data_task = BigQueryCreateExternalTableOperator(
#         task_id="gcs_2_bq_clean_data_task",
#         table_resource={
#             "tableReference": {
#                 "projectId": PROJECT_ID,
#                 "datasetId": BIGQUERY_DATASET,
#                 # "tableId": "external_table_yellowtrip_2019",
#                 "tableId": "clean_data_amazon",
#             },
#             "externalDataConfiguration": {
#                 # "sourceFormat": "PARQUET",
#                 "sourceUris": [f"gs://{BUCKET}/clean_amazon_data_zoomcamp/*"],
#             },
#         },
#     )

#     gcs_2_bq_raw_data_task = BigQueryCreateExternalTableOperator(
#         task_id="gcs_2_bq_raw_data_task",
#         table_resource={
#             "tableReference": {
#                 "projectId": PROJECT_ID,
#                 "datasetId": BIGQUERY_DATASET,
#                 # "tableId": "external_table_yellowtrip_2019",
#                 "tableId": "raw_data_amazon",
#             },
#             "externalDataConfiguration": {
#                 # "sourceFormat": "PARQUET",
#                 "sourceUris": [f"gs://{BUCKET}/raw_amazon_data_zoomcamp/*"],
#             },
#         },
#     )

#     gcs_2_bq_clean_data_task
#     gcs_2_bq_raw_data_task

#     # CREATE_PART_TBL_QUERY = f"CREATE OR REPLACE TABLE `de-zoomcamp-339108.trips_data_all.yellow_trip_data_partition_af` \
#     #                         PARTITION BY \
#     #                         DATE(tpep_pickup_datetime) AS \
#     #                         SELECT * FROM `{BIGQUERY_DATASET}.external_table_yellowtrip_2019`"

#     # bq_ext_2_partition_task = BigQueryInsertJobOperator(
#     #     task_id="bq_ext_2_partition_task",
#     #     configuration={
#     #         "query": {
#     #             "query": CREATE_PART_TBL_QUERY,
#     #             "useLegacySql": False,
#     #         }
#     #     },
#     # )

#     # gcs_2_gcs_task >> gcs_2_bq_ext_task >> bq_ext_2_partition_task