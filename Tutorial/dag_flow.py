from airflow import DAG
from datetime import timedelta, datetime
from shared.sample import add, multiply, condition_check
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}
dag = DAG('example1', schedule_interval=None, default_args=default_args)

t1 = BashOperator(
    task_id='start',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='step1',
    bash_command='sleep 5',
    trigger_rule="one_success",
    dag=dag,
)

t3 = BranchPythonOperator(
            task_id='condition_check',
            python_callable=condition_check,
            trigger_rule="one_success",
            dag=dag)


t4 = PythonOperator(
            task_id='step2',
            provide_context=True,
            op_args=[10,20],
            python_callable=add,
            xcom_push=True,
            trigger_rule="one_success",
            dag=dag,)


t5 = BashOperator(
    task_id='step3',
    bash_command='date',
    dag=dag,
)


t6 = PythonOperator(
            task_id='here',
            provide_context=True,
            op_args=[10,0],
            python_callable=multiply,
            xcom_push=True,
            trigger_rule="one_success",
            dag=dag,)


t7 = BashOperator(
    task_id='send_email_as_fail',
    bash_command='date',
    trigger_rule="one_failed",
    dag=dag,
)


t8 = BashOperator(
    task_id='send_email_as_pass',
    bash_command='date',
    trigger_rule="one_success",
    dag=dag,
)

# Helper function 

def get_records(task_id,**kwargs):
    ti = kwargs['ti']
    xcom = ti.xcom_pull(task_ids=task_id)
    print("--------------------------------------")
    print(xcom)
    print("--------------------------------------")


t9 = PythonOperator(
            task_id='end',
            provide_context=True,
            python_callable=get_records,
            op_args=["step2"],
            trigger_rule="all_done",
            xcom_push=True,
            dag=dag)



# Dependencies 

t1 >> t2 >> t3
t3 >> [t4,t5]
t4 >> t6
t5 >> t6
t6 >> [t7, t8]
t7 >> t9
t8 >> t9