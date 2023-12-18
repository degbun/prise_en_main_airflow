from datetime import datetime, timedelta


from airflow.decorators import dag, task


default_args = {
    'owner': 'josue',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
@dag(
    dag_id='dag_with_taskflow_api_v2',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2021, 7, 29, 2),
    schedule_interval='@daily'
)

def hello_world():


    @task()
    def get_age():
        return 19
    
    @task(multiple_outputs = True)
    def get_name():
        return{'last_name': 'josue','first_name':'degbun'}
    
    @task()
    def greet(first_name,last_name, age):
        print(f"hello world my name is {first_name} {last_name} and  i am {age}")

    name_dict =get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'], age =age)

greet_dag = hello_world()



