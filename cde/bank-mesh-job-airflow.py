from airflow import DAG
from airflow.utils import timezone
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from datetime import timedelta
from dateutil import parser

dag = DAG(
    dag_id='job_mesh_c1fb3a13',
    start_date=parser.isoparse('2025-03-12T20:16:59Z').replace(tzinfo=timezone.utc),
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args={
        'owner': 'jcaseiro',
    },
)

Tables_creation = CDEJobRunOperator(
    job_name='job-100',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='Tables_creation',
    dag=dag,
)

New_ingestion = CDEJobRunOperator(
    job_name='job-101',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='New_ingestion',
    dag=dag,
)

Second_ingestion = CDEJobRunOperator(
    job_name='job-101',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='Second_ingestion',
    dag=dag,
)

Running_simple_queries = CDEJobRunOperator(
    job_name='job-102',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='Running_simple_queries',
    dag=dag,
)

Running_complex_queries = CDEJobRunOperator(
    job_name='job-103',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='Running_complex_queries',
    dag=dag,
)

In_place_Iceberg_migration = CDEJobRunOperator(
    job_name='job-104',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='In-place_Iceberg_migration',
    dag=dag,
)

Cleaning = CDEJobRunOperator(
    job_name='job-clean',
    task_id='Cleaning',
    dag=dag,
)

Tables_creation << [Cleaning]
New_ingestion << [Tables_creation]
Second_ingestion << [New_ingestion]
Running_simple_queries << [Second_ingestion]
Running_complex_queries << [Second_ingestion]
In_place_Iceberg_migration << [Running_simple_queries, Running_complex_queries]