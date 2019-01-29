# -*- coding: utf-8 -*-
import datetime
import os
import googleapiclient.discovery
from airflow import models
from google.cloud import storage
from airflow.operators import BashOperator,BaseSensorOperator
from airflow import DAG
 
bucketList=[]
storage_client = storage.Client()
for bucket in storage_client.list_buckets():
	bucketList.append(bucket.name)
	
	
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
	

#Custom sensor which checks wheather the bucket exist or not. If exist it will return false else true.
#Once it finds that bucekt does not exist, it will create one.

class BucketSensor(BaseSensorOperator):
	def __init__(self,bucketName,*args,**kwargs):
		self.bucketName=bucketName
		super(BucketSensor, self).__init__(*args, **kwargs)

	def poke(self,context):
		bucket=self.bucketName
		if bucket not in bucketList:
			bucketList.add(bucket)
			return True
		else:
			return False
    


default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

with models.DAG(
        'Create Bucket',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
			
	createBucket = BashOperator(
	task_id='createBucket',
	bash_command='gsutil mb gs://'+models.Variable.get('BucketName'),
	dag=dag)
	
	IsBucketExists = BucketSensor(
		task_id='IsBucketExists',
		bucketName=models.Variable.get('BucketName'),
		dag=dag)


createBucket.set_upstream(IsBucketExists)


