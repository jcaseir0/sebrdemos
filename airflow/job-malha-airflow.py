from airflow import DAG
from airflow.utils import timezone
from airflow.operators.dummy_operator import DummyOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from datetime import timedelta
from dateutil import parser
import sys

# --- Variable Collection and Configuration Block ---
# Accesses the first argument passed (sys.argv[1]), if it exists.
# If there is no argument (only sys.argv[0] - the script name),
# sets username as an empty string ("").
username_arg = sys.argv[1] if len(sys.argv) > 1 else ""

# Builds the suffix of the job name and dag_id
# If username_arg is not empty, the suffix will be '_' + username_arg (e.g., '_user001')
# If username_arg is empty, the suffix will be empty ("")
username_suffix = f"_{username_arg}" if username_arg else ""

dag = DAG(
    dag_id='malha_airflow'+ username_suffix,
    start_date=parser.isoparse('2025-07-10T13:00:18Z').replace(tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args={
        'owner': username_arg if username_arg else 'jcaseiro',
    },
)

start = DummyOperator(
        task_id="start",
        dag=dag
)

create_table = CDEJobRunOperator(
    job_name=f'create-table{username_suffix}',
    depends_on_past=False,
    trigger_rule='all_success',
    task_id='create_table',
    dag=dag,
)

create_table_validation = CDEJobRunOperator(
    job_name=f'create-table-validation{username_suffix}',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='create_table_validation',
    dag=dag,
)

insert_table = CDEJobRunOperator(
    job_name=f'insert-table{username_suffix}',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='insert_table',
    dag=dag,
)

insert_table_validation = CDEJobRunOperator(
    job_name=f'insert-table-validation{username_suffix}',
    depends_on_past=True,
    trigger_rule='all_success',
    task_id='insert_table_validation',
    dag=dag,
)

end = DummyOperator(
        task_id="end",
        dag=dag
)

# Definition of Execution Order (Data Lineage)
start >> create_table >> create_table_validation >> insert_table >> insert_table_validation >> end
