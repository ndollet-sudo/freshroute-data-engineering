"""Shared helpers: logs, Snowflake (key pair) and Postgres connections, HTTP with retry, API payload loading."""
import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import requests
import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

# Run id written in every bronze row loaded by Python: lets us trace and replay a run (set by run_pipeline.sh / Airflow)
RUN_ID = os.environ.get("RUN_ID") or datetime.now(timezone.utc).strftime("manual_%Y%m%dT%H%M%S")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def sf_connect(schema: str = "BRONZE"):
    """Snowflake connection with key pair auth: a scheduled job cannot type a password or approve MFA."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ.get("SNOWFLAKE_ROLE", "FRESHROUTE_ENGINEER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "FRESHROUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "FRESHROUTE"),
        schema=schema,
    )


def pg_connect():
    """The on-prem operational database (Postgres in Docker)."""
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"), port=os.environ.get("PG_PORT", 5432),
        dbname=os.environ.get("PG_DB", "freshroute_ops"),
        user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"],
    )


def http_get_json(url: str, params: dict | None = None, retries: int = 3, timeout: int = 30):
    """GET with timeout + retry + exponential backoff, then fail loudly (incident 1: API down / HTTP 429)."""
    log = get_logger("http")
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.warning("Attempt %s/%s failed for %s: %s", attempt, retries, url, exc)
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)


def require_keys(payload: dict, required: list[str], source: str) -> None:
    """Fail early if the API changed its format (schema drift)."""
    missing = [k for k in required if k not in payload]
    if missing:
        raise ValueError(f"[{source}] schema drift: missing keys {missing}")


def replace_payload(cur, table: str, source_key: str, payload) -> None:
    """Idempotent load of one API answer: delete the previous payload for the same key, insert the new one.
    Replaying the script never creates duplicates."""
    cur.execute(f"DELETE FROM {table} WHERE source_key = %s", (source_key,))
    cur.execute(
        f"INSERT INTO {table} (source_key, payload, _run_id, _loaded_at) "
        "SELECT %s, PARSE_JSON(%s), %s, CURRENT_TIMESTAMP()",
        (source_key, json.dumps(payload), RUN_ID),
    )
