from airflow import DAG
from airflow.utils import timezone
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from dateutil import parser

dag = DAG(
    dag_id='job_mesh',
    start_date=parser.isoparse('2025-03-21T14:50:45Z').replace(tzinfo=timezone.utc),
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args={
        'owner': 'jcaseiro',
    },
)

Create_Tables = CDEJobRunOperator(
    job_name='job-100',
    task_id='Create_Tables',
    dag=dag,
)

Partition_and_Bucketing_Ingestion = CDEJobRunOperator(
    job_name='job-101',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='Partition_and_Bucketing_Ingestion',
    dag=dag,
)

Simple_Queries = CDEJobRunOperator(
    job_name='job-102',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='Simple_Queries',
    dag=dag,
)

Complex_Queries = CDEJobRunOperator(
    job_name='job-103',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='Complex_Queries',
    dag=dag,
)

Iceberg_Migration_InPlace = CDEJobRunOperator(
    job_name='job-104',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='Iceberg_Migration_InPlace',
    dag=dag,
)

Partition_and_Bucketing_Ingestion << [Create_Tables]
Simple_Queries << [Partition_and_Bucketing_Ingestion]
Complex_Queries << [Partition_and_Bucketing_Ingestion]
Iceberg_Migration_InPlace << [Simple_Queries, Complex_Queries]