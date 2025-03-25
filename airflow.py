from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import random

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'complex_example_dag',
    default_args=default_args,
    description='A complex DAG example with branching and dependencies',
    schedule_interval='@daily',
    catchup=False,
)

# Task 1: Start Execution
def start_execution():
    print("Starting DAG execution...")

task_start = PythonOperator(
    task_id='start_execution',
    python_callable=start_execution,
    dag=dag,
)

# Task 2: Processing Data
def process_data():
    print("Processing Data...")
    value = random.randint(1, 100)
    print(f"Generated Value: {value}")
    return value

task_process = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

# Task 3: Conditional Branching
def decide_next_step(**kwargs):
    task_instance = kwargs['ti']
    value = task_instance.xcom_pull(task_ids='process_data')
    if value > 50:
        return 'high_value_task'
    else:
        return 'low_value_task'

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_next_step,
    provide_context=True,
    dag=dag,
)

# Task 4A: If value > 50, execute this
task_high = BashOperator(
    task_id='high_value_task',
    bash_command='echo "Value is high!"',
    dag=dag,
)

# Task 4B: If value <= 50, execute this
task_low = BashOperator(
    task_id='low_value_task',
    bash_command='echo "Value is low!"',
    dag=dag,
)

# Task 5: End Execution
def end_execution():
    print("DAG execution completed!")

task_end = PythonOperator(
    task_id='end_execution',
    python_callable=end_execution,
    dag=dag,
)

# Define task dependencies
task_start >> task_process >> branch_task
branch_task >> [task_high, task_low] >> task_end