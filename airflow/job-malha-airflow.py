from airflow import DAG
from airflow.utils import timezone
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from datetime import timedelta
from dateutil import parser

dag = DAG(
    dag_id='malha_airflow',
    start_date=parser.isoparse('2025-07-10T13:00:18Z').replace(tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args={
        'owner': 'jcaseiro',
    },
)

create_table = CDEJobRunOperator(
    job_name='create-table',
    depends_on_past=False,
    trigger_rule='all_success',
    task_id='create_table',
    dag=dag,
)

create_table_validation = CDEJobRunOperator(
    job_name='create-table-validation',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='create_table_validation',
    dag=dag,
)

insert_table = CDEJobRunOperator(
    job_name='insert-table',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='insert_table',
    dag=dag,
)

insert_table_validation = CDEJobRunOperator(
    job_name='insert-table-validation',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='insert_table_validation',
    dag=dag,
)

create_table_validation << [create_table]
insert_table << [create_table_validation]
insert_table_validation << [insert_table]
