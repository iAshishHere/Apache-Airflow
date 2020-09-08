from airflow import DAG
from airflow import models
import datetime
from airflow.operators import BashOperator,BaseSensorOperator

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

elementList = [1,2,3]

class ListSensor(BaseSensorOperator):
	def __init__(self,elementName,*args,**kwargs):
		self.elementName = elementName
		super(ListSensor, self).__init__(*args, **kwargs)

	def poke(self,context):
		element=self.elementName
		if element not in elementList:
			return True
		else:
			return False
    

default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with models.DAG('CheckListSensor', schedule_interval=datetime.timedelta(days=1), default_args=default_dag_args) as dag:
			

    createBucket = BashOperator(
	task_id='yes_it_exists',
	bash_command='echo DONE',
	dag=dag)


    IsBucketExists = ListSensor(
		task_id='isElementExist',
		elementName=10,
		dag=dag)



createBucket.set_upstream(IsBucketExists)