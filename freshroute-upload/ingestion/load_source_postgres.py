"""Simulates the on-prem operational database: writes one batch (= one day) of deliveries into Postgres.

  python ingestion/load_source_postgres.py --batch 1     # day 1 (initial load)
  python ingestion/load_source_postgres.py --batch 2     # day 2: new rows, updates of old rows, schema drift

Like a legacy system: every column is TEXT. Adapted from the course pack (04-source-databases/load_to_postgres.py).
Each row gets _pg_loaded_at = now(): this is the column the incremental job uses as its watermark.
"""
import argparse
import csv
from pathlib import Path

from psycopg2 import sql

from common import get_logger, pg_connect

log = get_logger("source_pg")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, required=True)
    ap.add_argument("--data", default="data/freshroute")
    a = ap.parse_args()
    path = Path(a.data) / f"batch_{a.batch:03d}" / "deliveries.csv"

    with open(path, newline="", encoding="utf-8") as fh:
        header = next(csv.reader(fh))

    table = sql.Identifier("ops", "deliveries")
    with pg_connect() as con, con.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ops")
        cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (_batch int, _pg_loaded_at timestamptz DEFAULT clock_timestamp())")
                    .format(table))
        cur.execute(sql.SQL("SELECT count(*) FROM {} WHERE _batch = %s").format(table), (a.batch,))
        if cur.fetchone()[0]:
            log.info("Batch %s already in Postgres, nothing to do", a.batch)
            return
        for c in header:   # schema drift: a new column is added instead of failing
            cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} text").format(table, sql.Identifier(c)))
        cur.execute(sql.SQL("CREATE TEMP TABLE incoming ({}) ON COMMIT DROP").format(
            sql.SQL(", ").join(sql.SQL("{} text").format(sql.Identifier(c)) for c in header)))
        with open(path, encoding="utf-8") as fh:
            cur.copy_expert("COPY incoming FROM STDIN WITH (FORMAT csv, HEADER true)", fh)
        cols = sql.SQL(", ").join(map(sql.Identifier, header))
        cur.execute(sql.SQL("INSERT INTO {} ({}, _batch) SELECT {}, %s FROM incoming").format(table, cols, cols),
                    (a.batch,))
        log.info("Batch %s: %s deliveries written to ops.deliveries", a.batch, cur.rowcount)


if __name__ == "__main__":
    main()
