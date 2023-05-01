# import libraries
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import pendulum
from src.etl import*
from airflow.operators.python_operator import PythonOperator

"""
This dag will run load_contract_offer"""

#initializing pendulum for timezone management
local_timezone=pendulum.timezone("Africa/Nairobi")
# Defination of default dag arguments
default_args={
    'owner':'ADI',
    'depends_on_post':False,
    # 'email':['@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5)
}
dag=DAG(
    dag_id='Tupande',
    default_args=default_args,
    start_date=datetime(2021,11,19,tzinfo=local_timezone),
    schedule_interval="0 18 * * 5",
    template_searchpath="/opt/ETL_Project"
)



load_contract_offer = PythonOperator(
    task_id='run_python',
    python_callable=load_contract_offer,
    dag=dag
)
load_contract_payments = PythonOperator(
    task_id='run_python',
    python_callable=load_contract_offer,
    dag=dag
)
load_contracts = PythonOperator(
    task_id='run_python',
    python_callable=load_contract_offer,
    dag=dag
)
load_leads = PythonOperator(
    task_id='run_python',
    python_callable=load_contract_offer,
    dag=dag
)
tupande_dataset = PythonOperator(
    task_id='run_python',
    python_callable=load_contract_offer,
    dag=dag
)

load_contract_offer>>load_contract_payments>>load_contracts>>load_leads>>tupande_dataset