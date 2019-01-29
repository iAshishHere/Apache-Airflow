import datetime
import logging
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from google.cloud import storage


client = storage.Client()
bucket = client.get_bucket(str(models.Variable.get('composer_bucket')))

#reading temporary_dict json
configblob = bucket.get_blob("test_sla/tasks/configs/temp_dict.json")
temp_dict = eval(configblob.download_as_string().decode())

#reading config_dict
configblob = bucket.get_blob("test_sla/tasks/configs/cluster_default.json")
config_dict = eval(configblob.download_as_string().decode())


class MySqlGetOperator(MySqlOperator):
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
                         schema=self.database)
        return hook.get_records(
            self.sql,
            parameters=self.parameters)


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
task_execution_ts=0
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

#create dag
with models.DAG(
        'SLASample1',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    t0 =BashOperator(
        task_id='task1',
        bash_command='sleep 5',
        dag=dag)

    t1 = MySqlGetOperator(
        task_id='Collect_Task_Ts',
        mysql_conn_id='airflow_db',
        sql="select task_id, dag_id, cast(execution_date as char), cast(start_date as char), cast(end_date as char), duration, state from task_instance where TIMEDIFF(current_timestamp(6), start_date) <= '03:00:00.000000'",
        dag=dag)


    def get_records(**kwargs):

        ti = kwargs['ti']
        xcom = ti.xcom_pull(task_ids='Collect_Task_Ts')
        for element in xcom:
            temp_dict.update({element[0]:element})

        for task_id,values in config_dict.items():
            for key,val in values.items():
                expected_sla=val['SLA']

        for task_id, value in temp_dict.items():
            actual_execution_time=value[5]

        if(expected_sla>actual_execution_time):
            print("do something")



    def check_sla(execution_ts,sla):
        if execution_ts>sla:
            logging.info("SLA Missed")
        else:
            logging.info("SLA not Missed")


    t2 = PythonOperator(
        task_id='get_task_execution_ts',
        provide_context=True,
        python_callable=get_records,
        dag=dag)

    t0 >> t1 >> t2

