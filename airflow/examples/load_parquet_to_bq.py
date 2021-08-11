import datetime
import os
import logging

from airflow import models
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import gcs_to_bq
#from airflow.operators import dummy_operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
 
from airflow.contrib.operators.dataproc_operator import DataprocClusterScaleOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator
from airflow.contrib.operators.dataproc_operator import DataProcSparkSqlOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator

from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import BashOperator

# Import operator from plugins
from airflow.contrib.operators import gcs_to_gcs
from airflow.utils import trigger_rule

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'retries': 0,
    'project_id': models.Variable.get('gcp_project')
}


with models.DAG( 
        'bq_poc',
        schedule_interval=None, 
        default_args=default_dag_args) as dag:


	load_to_bq_from_gcs = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
	task_id='load_to_bq_from_gcs',
		source_objects=[
            	'part*'
        	],
		source_format='PARQUET',
		skip_leading_rows=1,
		write_disposition='WRITE_TRUNCATE', #overwrite? WRITE_APPEND to append
		bucket='mt-modeling/google_audit_original_all/dt=2021-08-10', #nootice end with no / and and doest start with gs://
		destination_project_dataset_table='datawarehouse-319014.DL_TEMP.test_for_pablo1',
		default_args=default_dag_args,
		dag=dag
	)
