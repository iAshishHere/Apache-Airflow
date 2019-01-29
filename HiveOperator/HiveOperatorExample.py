
import datetime
import os

from airflow import models
import googleapiclient.discovery
from airflow.contrib.operators import dataproc_operator
from airflow.operators import BashOperator,PythonOperator
from airflow.utils import trigger_rule
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator



yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}



with models.DAG(
        'composer_sample_quickstart',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

	
	HiveInsertingTable = DataProcHiveOperator(
	task_id='HiveInsertingTable',
	gcp_conn_id='google_cloud_default',
	query="CREATE EXTERNAL TABLE trades_sample6(trading_date_time TIMESTAMP,network CHAR(1),message_category CHAR(1),message_type CHAR(1),message_sequence BIGINT,market_exchange CHAR(1),symbol VARCHAR(10),trade_price DOUBLE,trade_size BIGINT,trade_conditions VARCHAR(6),trade_conditions2 VARCHAR(6) )ROW FORMAT DELIMITED FIELDS TERMINATED BY ','LOCATION 'gs://market-data11-bucket/data/';",
	cluster_name='YourClusterName',
	region='us-central1',
	dag=dag)
	
	QuerytoGS = DataProcHiveOperator(
	task_id='QuerytoGS',
	gcp_conn_id='google_cloud_default',
	query="INSERT OVERWRITE DIRECTORY 'gs://market-data11-bucket/output/' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' SELECT * FROM trades_sample6;",
	cluster_name='YourClusterName',
	region='us-central1',
	dag=dag)
	
	
	
	QuerytoGS.set_upstream(HiveInsertingTable)

	
	
