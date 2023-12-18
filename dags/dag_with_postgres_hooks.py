from datetime import datetime,timedelta
import csv
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def postgres_to_s3():
     hook = PostgresHook(postgres_conn_id="postgres_localhost")
     conn = hook.get_conn()
     cursor =conn.cursor()
     cursor.execute("SELECT * FROM orders WHERE date <= '2022-05-01'")

     with open(f"dags/get_orders.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
        logging.info("Saved orders data in text file get_orders.txt")






with DAG(
    dag_id='dag_with_postgres_hooks_v1',
    default_args=default_args,
    start_date=datetime(2021, 12, 19),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )
    task1