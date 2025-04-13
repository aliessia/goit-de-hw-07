from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import random
import time

def choose_medal_type():
    return random.choice(['gold_task', 'silver_task', 'bronze_task'])

def create_table():
    print("Creating medals_result table if not exists")

def process_medal(medal_type):
    print(f"Processing {medal_type} medal...")

def delay_function():
    time.sleep(35)

def check_recent_insert_func():
    print("Checking recent insert... (dummy sensor)")
    return True

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    dag_id='olympic_medals_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example']
) as dag:

    start = EmptyOperator(task_id='start')

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    branch_medal_type = BranchPythonOperator(
        task_id='branch_medal_type',
        python_callable=choose_medal_type
    )

    gold_task = PythonOperator(
        task_id='gold_task',
        python_callable=lambda: process_medal('Gold')
    )

    silver_task = PythonOperator(
        task_id='silver_task',
        python_callable=lambda: process_medal('Silver')
    )

    bronze_task = PythonOperator(
        task_id='bronze_task',
        python_callable=lambda: process_medal('Bronze')
    )

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=delay_function,
        trigger_rule=TriggerRule.ONE_SUCCESS  # важливо
    )

    check_recent_insert = PythonSensor(
        task_id='check_recent_insert',
        python_callable=check_recent_insert_func,
        mode='poke',
        poke_interval=5,
        timeout=60,
        trigger_rule=TriggerRule.ONE_SUCCESS  # важливо
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ONE_SUCCESS  # важливо
    )

    # Зв'язки
    start >> create_table_task >> branch_medal_type
    branch_medal_type >> [gold_task, silver_task, bronze_task]
    [gold_task, silver_task, bronze_task] >> delay_task >> check_recent_insert >> end
