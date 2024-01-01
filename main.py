import sys
import os

sys.path.append('/Users/shiryaevva/HSE/2-nd_year/DE/HSE_DE_Project/py_scripts')

import logging
from py_scripts.functions import archive_file
from py_scripts.change_data_capture import cdc_accounts, cdc_cards, cdc_clients, cdc_passport_blacklist, cdc_terminals, cdc_transactions

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'Victoria Shiryaeva',
    'email': 'vi.shiryaevva@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(
    dag_id='ETL_HSE_DE',
    schedule_interval='00 06 * * *',
    start_date=days_ago(2),
    catchup=False,
    tags=['hse_de'],
    default_args=DEFAULT_ARGS,
    description='Daily ETL process for reporting fraud.',
)


def init() -> None:
    logger.info(f'ETL process started at {datetime.now()}')


def dwh_finish() -> None:
    logger.info(f'Increment downloading to DWH finished at {datetime.now()}')


def archive_file_terminals(**kwargs) -> None:    
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='cdc_terminals')
    archive_file(result['directory_path'], result['file'])

def archive_file_transactions(**kwargs) -> None:    
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='cdc_transactions')
    archive_file(result['directory_path'], result['file'])

def archive_file_passport_blacklist(**kwargs) -> None:    
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='cdc_passport_blacklist')
    archive_file(result['directory_path'], result['file'])


# Инициализация ETL
task_init = PythonOperator(
    task_id='init', 
    python_callable=init, 
    dag=dag
)

# Заливка данных с источников в stage
task_cdc_accounts = PythonOperator(
    task_id='cdc_accounts',
    python_callable=cdc_accounts,
    dag=dag,
)

task_cdc_cards = PythonOperator(
    task_id='cdc_cards',
    python_callable=cdc_cards,
    dag=dag,
)

task_cdc_clients = PythonOperator(
    task_id='cdc_clients',
    python_callable=cdc_clients,
    dag=dag,
)

task_cdc_terminals = PythonOperator(
    task_id='cdc_terminals',
    python_callable=cdc_terminals,
    dag=dag,
)

task_cdc_transactions = PythonOperator(
    task_id='cdc_transactions',
    python_callable=cdc_transactions,
    dag=dag,
)

task_cdc_passport_blacklist = PythonOperator(
    task_id='cdc_passport_blacklist',
    python_callable=cdc_passport_blacklist,
    dag=dag,
)

# Архивирование источников
task_archive_file_terminals = PythonOperator(
    task_id='archive_file_terminals',
    python_callable=archive_file_terminals,
    provide_context=True,
    dag=dag,
)

task_archive_file_transactions = PythonOperator(
    task_id='archive_file_transactions',
    python_callable=archive_file_transactions,
    provide_context=True,
    dag=dag,
)

task_archive_file_passport_blacklist = PythonOperator(
    task_id='archive_file_passport_blacklist',
    python_callable=archive_file_passport_blacklist,
    provide_context=True,
    dag=dag,
)


# Заливка инкрементов в DWH 
task_inc_accounts_hist = PostgresOperator(
    task_id='inc_accounts_hist',
    postgres_conn_id='conn_postgres_hse_de',
    sql='./sql_scripts/inc_accounts_hist.sql',
    dag=dag,
)

task_inc_cards_hist = PostgresOperator(
    task_id='inc_cards_hist',
    postgres_conn_id='conn_postgres_hse_de',
    sql='./sql_scripts/inc_cards_hist.sql',
    dag=dag,
)

task_inc_clients_hist = PostgresOperator(
    task_id='inc_clients_hist',
    postgres_conn_id='conn_postgres_hse_de',
    sql='./sql_scripts/inc_clients_hist.sql',
    dag=dag,
)

task_inc_terminals_hist = PostgresOperator(
    task_id='inc_terminals_hist',
    postgres_conn_id='conn_postgres_hse_de',
    sql='./sql_scripts/inc_terminals_hist.sql',
    dag=dag,
)

task_inc_transactions = PostgresOperator(
    task_id='inc_transactions',
    postgres_conn_id='conn_postgres_hse_de',
    sql='./sql_scripts/inc_transactions.sql',
    dag=dag,
)

task_inc_passport_blacklist = PostgresOperator(
    task_id='inc_passport_blacklist',
    postgres_conn_id='conn_postgres_hse_de',
    sql='./sql_scripts/inc_passport_blacklist.sql',
    dag=dag,
)

# Завершение ETL 
task_dwh_finish = PythonOperator(
    task_id='dwh_finish', 
    python_callable=dwh_finish, 
    dag=dag
)

# Создание отчета rep_fraud 
task_report_fraud = PostgresOperator(
    task_id='report_fraud',
    postgres_conn_id='conn_postgres_hse_de',
    sql='./sql_scripts/report_fraud.sql',
    dag=dag,
)

task_init >> task_cdc_accounts >> task_inc_accounts_hist >> task_dwh_finish >> task_report_fraud
task_init >> task_cdc_cards >> task_inc_cards_hist >> task_dwh_finish >> task_report_fraud
task_init >> task_cdc_clients >> task_inc_clients_hist >> task_dwh_finish >> task_report_fraud
task_init >> task_cdc_terminals >> [task_archive_file_terminals, task_inc_terminals_hist]
task_init >> task_cdc_transactions >> [task_archive_file_transactions, task_inc_transactions]
task_init >> task_cdc_passport_blacklist >> [task_archive_file_passport_blacklist, task_inc_passport_blacklist]

task_archive_file_terminals >> task_dwh_finish
task_archive_file_transactions >> task_dwh_finish
task_archive_file_passport_blacklist >> task_dwh_finish

task_inc_terminals_hist >> task_dwh_finish >> task_report_fraud
task_inc_transactions >> task_dwh_finish >> task_report_fraud
task_inc_passport_blacklist >> task_dwh_finish >> task_report_fraud
