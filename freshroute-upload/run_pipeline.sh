#!/usr/bin/env bash
# Runs one day of the FreshRoute pipeline locally (simple orchestration, used for the live demo).
#   ./run_pipeline.sh 1        # day 1 = batch 1 (initial load)
#   ./run_pipeline.sh 2        # day 2 = new rows, updates, schema drift
# set -e: stop at the first error, like Airflow would; the trap sends an alert with the failed step.
set -euo pipefail

BATCH="${1:-1}"
export RUN_ID="run_$(date -u +%Y%m%dT%H%M%SZ)_b${BATCH}"
RUN="docker compose run --rm -e RUN_ID=${RUN_ID} ingest"
STEP="start"
mkdir -p logs
LOG="logs/${RUN_ID}.log"
trap '$RUN python ingestion/notify.py "FreshRoute pipeline FAILED at step ${STEP} (run ${RUN_ID}, log ${LOG})"' ERR

{
  echo "=== ${RUN_ID} ==="
  STEP="source_system";   $RUN python ingestion/load_source_postgres.py --batch "$BATCH"
  STEP="files_batch";     $RUN python ingestion/load_files_batch.py --batch "$BATCH"
  STEP="deliveries_incr"; $RUN python ingestion/load_deliveries_incremental.py
  STEP="gps_stream";      $RUN python ingestion/stream_gps_pings.py --batch "$BATCH" --chunk 2000 --sleep 0.5
  STEP="weather_api";     $RUN python ingestion/load_weather_api.py
  STEP="fx_api";          $RUN python ingestion/load_fx_api.py
  STEP="holidays_api";    $RUN python ingestion/load_bank_holidays_api.py
  STEP="dbt_build";       $RUN sh -c 'cd dbt/freshroute && dbt seed --profiles-dir . && dbt build --profiles-dir .'
  STEP="dbt_freshness";   $RUN sh -c 'cd dbt/freshroute && dbt source freshness --profiles-dir .'
  echo "=== OK ${RUN_ID} ==="
} 2>&1 | tee "$LOG"
