from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

from airflow.contrib.hooks.databricks_hook import DatabricksHook

def get_job_id_by_name(job_name: str, databricks_conn_id: str) -> str:
    list_endpoint = ('GET', 'api/2.0/jobs/list')
    hook = DatabricksHook(databricks_conn_id=databricks_conn_id)
    response_payload = hook._do_api_call(list_endpoint, {})
    all_jobs = response_payload.get("jobs", [])
    matching_jobs = [j for j in all_jobs if j["settings"]["name"] == job_name]

    if not matching_jobs:
        raise Exception(f"Job with name {job_name} not found")

    if len(matching_jobs) > 1:
        raise Exception(f"Job with name {job_name} is duplicated. Please make job name unique in Databricks UI.")

    job_id = matching_jobs[0]["job_id"]
    return job_id

default_args = {
  'owner': 'airflow'
}

with DAG('mlops_demo_dag',
  start_date = days_ago(2),
  schedule_interval = None,
  default_args = default_args
  ) as dag:

    job_id = get_job_id_by_name("shenlin_mlops_demo", "databricks_default")
    operator = DatabricksRunNowOperator(
        job_id=job_id,
        task_id = 'run_now',
        databricks_conn_id = 'databricks_default'
    )


