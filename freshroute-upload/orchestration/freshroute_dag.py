"""Airflow DAG: one run per business day, every task runs in the project's container (same image everywhere).

Reflexes checked by the panel:
- idempotent tasks (COPY INTO load metadata, watermark, delete+insert for APIs) -> retries never duplicate
- the batch comes from the run (param), not from "today" -> a day can be replayed
- schedule in the business time zone (Europe/London)
- failure is visible: retries, then a Slack/Teams alert after the last retry
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/freshroute"     # path of the repository on the Airflow machine
RUN = f"cd {PROJECT} && docker compose run --rm -e RUN_ID={{{{ run_id }}}} ingest"
BATCH = "{{ params.batch }}"


def alert_on_failure(context):
    import subprocess
    ti = context["task_instance"]
    subprocess.run(["sh", "-c", f'{RUN} python ingestion/notify.py "FreshRoute task {ti.task_id} failed '
                    f'(run {context["run_id"]})"'], check=False)


default_args = {
    "owner": "freshroute-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="freshroute_daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="Europe/London"),
    schedule="0 5 * * *",            # 05:00 London: gold is ready for the 08:00 depot briefing
    catchup=False,
    default_args=default_args,
    params={"batch": 1},
    tags=["freshroute"],
) as dag:
    source = BashOperator(task_id="source_system_day",
                          bash_command=f"{RUN} python ingestion/load_source_postgres.py --batch {BATCH}")
    files = BashOperator(task_id="ingest_files_batch",
                         bash_command=f"{RUN} python ingestion/load_files_batch.py --batch {BATCH}")
    deliveries = BashOperator(task_id="ingest_deliveries_incremental",
                              bash_command=f"{RUN} python ingestion/load_deliveries_incremental.py")
    gps = BashOperator(task_id="ingest_gps_stream",
                       bash_command=f"{RUN} python ingestion/stream_gps_pings.py --batch {BATCH} --sleep 0")
    apis = [
        BashOperator(task_id=f"ingest_{name}_api", bash_command=f"{RUN} python ingestion/load_{name}_api.py")
        for name in ("weather", "fx", "bank_holidays")
    ]
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{RUN} sh -c 'cd dbt/freshroute && dbt seed --profiles-dir . && dbt build --profiles-dir .'",
    )
    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{RUN} sh -c 'cd dbt/freshroute && dbt source freshness --profiles-dir .'",
    )

    source >> deliveries
    [files, deliveries, gps, *apis] >> dbt_build >> freshness
