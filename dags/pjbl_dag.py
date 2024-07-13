from datetime import datetime
from airflow import DAG
from scrape import scrape_dbrealestate, scrape_winston, scrape_xavier
from airflow.operators.python import PythonOperator
from update_email import mail_update
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "start_date": datetime(2024, 6, 6),
    "owner": "azki"
}
dag = DAG(
    dag_id="pjbl_dag2",
    default_args=default_args,
    schedule_interval = "0 0 * * *"
)

scrape_realestate_task = PythonOperator(
    task_id='scrape_dbrealestate',
    python_callable =scrape_dbrealestate,
    dag=dag,
)

scrape_winston_task = PythonOperator(
    task_id="scrape_winston",
    python_callable = scrape_winston,
    dag=dag,
)

scrape_xavier_task = PythonOperator(
    task_id="scrape_xavier",
    python_callable=scrape_xavier,
    dag=dag,
)

update_task = PythonOperator(
    task_id='update_mail',
    python_callable=mail_update,
    dag=dag,
)

scrape_realestate_task >> scrape_winston_task >> scrape_xavier_task >> update_task