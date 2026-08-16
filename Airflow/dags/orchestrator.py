import pendulum
from airflow.sdk import dag,task



DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"
DBT_EXC = f'cd {DBT_DIR} && dbt'

default_args = {
    'depends_on_past':False,
    'retries':3,
    'retry_delay':pendulum.duration(minutes=1)
}

@dag(
    dag_id = "lakehouse_dbt",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date = pendulum.datetime(2026,8,16,tz="UTC"),
    catchup=False
)
def orchestrator():

    @task.bash
    def dbt_run_silver():
        return f"{DBT_EXC} run --select silver --profiles-dir {DBT_PROFILES_DIR} --log-path /tmp/dbt-logs"

    @task.bash
    def dbt_run_gold():
        return f"{DBT_EXC} run --select gold --profiles-dir {DBT_PROFILES_DIR} --log-path /tmp/dbt-logs"
 
    t1 =dbt_run_silver()
    t2 = dbt_run_gold()

    t1>>t2

orchestrator()