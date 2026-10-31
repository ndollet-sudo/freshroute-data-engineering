"""Source 2 - INCREMENTAL PULL: Postgres ops.deliveries -> BRONZE.DELIVERIES.

1. Watermark = MAX(_pg_loaded_at) already in Snowflake ("where did I stop?")
2. Extract only the delta: WHERE _pg_loaded_at > watermark
3. Append to bronze as received (text). Silver keeps the latest version of each delivery_id.

Replay-safe: a second run finds nothing newer than the watermark -> 0 row.
Schema drift: known renames are mapped (outcome_v2 -> outcome), unknown columns are logged as a warning.
"""
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from common import RUN_ID, get_logger, pg_connect, sf_connect

log = get_logger("deliveries_incr")

BRONZE_COLS = ["DELIVERY_ID", "ROUTE_ID", "CUSTOMER_REF", "PROMISED_BY", "DELIVERED_AT", "PARCEL_KG",
               "DELIVERY_FEE_GBP", "OUTCOME", "SOURCE_SYSTEM", "_BATCH", "_PG_LOADED_AT"]
RENAMES = {"OUTCOME_V2": "OUTCOME"}


def main():
    with sf_connect() as sf, sf.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(_pg_loaded_at), '1900-01-01'::TIMESTAMP_TZ) FROM BRONZE.DELIVERIES")
        watermark = cur.fetchone()[0]
        log.info("Watermark = %s", watermark)

        with pg_connect() as pg:
            delta = pd.read_sql("SELECT * FROM ops.deliveries WHERE _pg_loaded_at > %(wm)s ORDER BY _pg_loaded_at",
                                pg, params={"wm": watermark})
        log.info("Delta extracted: %s rows", len(delta))
        if delta.empty:
            return

        delta.columns = [c.upper() for c in delta.columns]
        if "OUTCOME" in delta and "OUTCOME_V2" in delta:     # batch 1 rows have outcome, batch 2+ outcome_v2
            delta["OUTCOME"] = delta["OUTCOME"].fillna(delta.pop("OUTCOME_V2"))
        delta = delta.rename(columns=RENAMES)
        unknown = sorted(set(delta.columns) - set(BRONZE_COLS))
        if unknown:
            log.warning("SCHEMA DRIFT: unknown column(s) %s ignored - update the mapping", unknown)
        for col in BRONZE_COLS:
            if col not in delta:
                delta[col] = None
        delta = delta[BRONZE_COLS].copy()
        delta["_PG_LOADED_AT"] = pd.to_datetime(delta["_PG_LOADED_AT"], utc=True)     # tz-aware, kept as a timestamp
        for col in BRONZE_COLS[:-2]:
            delta[col] = delta[col].astype("string")      # bronze = text, exactly as received
        delta["_RUN_ID"] = RUN_ID

        write_pandas(sf, delta, "DELIVERIES", schema="BRONZE", use_logical_type=True)
        log.info("Appended %s rows to BRONZE.DELIVERIES (run %s)", len(delta), RUN_ID)


if __name__ == "__main__":
    main()
