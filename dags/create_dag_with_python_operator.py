from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator




default_args = {
    'owner': 'josue',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


def get_name(ti):
    ti.xcom_push(key ='first_name', value='Degbun')
    ti.xcom_push(key ='last_name', value='josue')

def get_age(ti):
    ti.xcom_push(key ='age', value=19)
   


def greet(ti):
    first_name=ti.xcom_pull(task_ids='get_name',key = 'first_name')
    last_name=ti.xcom_pull(task_ids='get_name',key = 'last_name')
    age=ti.xcom_pull(task_ids='get_age',key = 'age')
    print(f"hello my  name is {first_name} {last_name}, "f"and  {age} years old")



with DAG(
    dag_id='our_first_python_operatorV6',
    default_args=default_args,
    description='This is our first dag usinng python operator',
    start_date=datetime(2021, 7, 29, 2),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable =greet,
        # op_kwargs={'age':20}
    )
    
    task2 = PythonOperator(

        task_id='get_name',
        python_callable=get_name
    )
    task3 = PythonOperator(

        task_id='get_age',
        python_callable=get_age
    )

    
    
    [task2,task3]>>task1 
