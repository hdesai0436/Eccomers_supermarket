from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator,DataprocStartClusterOperator,DataprocStopClusterOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 12),  # Set the desired start date
    'retries': 1
}

# Initialize the DAG
dag = DAG(
    'cloud_sql',  # The name of the DAG
    default_args=default_args,
    schedule_interval='@daily',  # Set the desired schedule interval
    catchup=False
)


    

# Function to interact with the database using PostgresHook
# Define the PythonOperator to execute the function
query_task = BashOperator(
    task_id='query_postgres_task',
    bash_command='python3 /home/airflow/gcs/dags/scripts/extract.py',
    dag=dag
)

# start_clutser = DataprocStartClusterOperator(
#     task_id = 'start_clutser',
#     project_id="hive-spark",
#     region = "us-central1",
#     cluster_name = "cluster-815e",
#     dag=dag
# )

# PYSPARK_JOB = {
#     "reference": {"project_id": "hive-spark"},
#     "placement": {"cluster_name": "cluster-815e"},
#     "pyspark_job": {"main_python_file_uri": "gs://ec-script/tran.py"},
# }

# submit_pyspark = DataprocSubmitJobOperator(
#     task_id='submit_pyspark_job',
#     job=PYSPARK_JOB,
#     region='us-central1',
#     project_id='hive-spark',
#     dag=dag,

# )

# stop_cluster = DataprocStopClusterOperator(
#      task_id = 'stop_clutser',
#     project_id="hive-spark",
#     region = "us-central1",
#     cluster_name = "cluster-815e",
#     dag=dag
# )
# # Define task dependencies (if you have multiple tasks)
# start_clutser >> submit_pyspark >> stop_cluster
query_task