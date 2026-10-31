"""INCIDENT 3 - a broken day arrives from the source system.

  python tools/inject_bad_batch.py            # writes batch 99 into Postgres: every delivery points to a route that does not exist
  -> the incremental job loads it into bronze, silver quarantines all of it (> 15 % of the rows),
     the test assert_quarantine_below_15_percent FAILS, dbt build SKIPS gold: yesterday's gold stays correct.

Recovery (see README, "Incidents"):  python tools/inject_bad_batch.py --rollback
  -> removes batch 99 from Postgres and from bronze, then rerun ./run_pipeline.sh: everything is green again.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

import pandas as pd  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

from common import get_logger, pg_connect, sf_connect  # noqa: E402

log = get_logger("inject")
BAD_BATCH = 99


def inject():
    src = pd.read_csv("data/freshroute/batch_001/deliveries.csv", dtype=str, keep_default_na=False).head(4000)
    src["delivery_id"] = (900000 + src.index).astype(str)      # new ids: do not overwrite real deliveries
    src["route_id"] = "999999"                                 # orphan foreign key on every row
    cols = ["delivery_id", "route_id", "customer_ref", "promised_by", "delivered_at", "parcel_kg",
            "delivery_fee_gbp", "outcome"]
    with pg_connect() as con, con.cursor() as cur:
        execute_values(cur, f"INSERT INTO ops.deliveries ({', '.join(cols)}, _batch) VALUES %s",
                       [tuple(r) + (BAD_BATCH,) for r in src[cols].itertuples(index=False)])
    log.warning("Injected %s broken deliveries as batch %s", len(src), BAD_BATCH)


def rollback():
    with pg_connect() as con, con.cursor() as cur:
        cur.execute("DELETE FROM ops.deliveries WHERE _batch = %s", (BAD_BATCH,))
        log.info("Postgres: %s rows removed", cur.rowcount)
    with sf_connect() as sf, sf.cursor() as cur:
        cur.execute("DELETE FROM BRONZE.DELIVERIES WHERE _batch = %s", (BAD_BATCH,))
        log.info("Snowflake bronze: %s rows removed", cur.fetchone()[0])


if __name__ == "__main__":
    rollback() if "--rollback" in sys.argv else inject()
