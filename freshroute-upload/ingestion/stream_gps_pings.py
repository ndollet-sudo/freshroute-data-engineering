"""Source 3 - MICRO-BATCH STREAM: GPS pings of the vans (JSON payload), sent in small chunks.

  python ingestion/stream_gps_pings.py --batch 1 --chunk 1000 --sleep 1

Simulates a telematics feed (the "Python simulating Kafka" method of the course): the pings of one day are
pushed to Snowflake every few seconds in small chunks. Each chunk is idempotent: rows already present
(same ping_id + same batch) are skipped, so a crash in the middle can be replayed safely.
The payload stays raw text in bronze (it is sometimes broken JSON or "NULL"); silver parses it.
"""
import argparse
import time
from pathlib import Path

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from common import RUN_ID, get_logger, sf_connect

log = get_logger("gps_stream")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, required=True)
    ap.add_argument("--chunk", type=int, default=1000)
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--data", default="data/freshroute")
    a = ap.parse_args()

    df = pd.read_csv(Path(a.data) / f"batch_{a.batch:03d}" / "gps_pings.csv", dtype=str, keep_default_na=False)
    df.columns = [c.upper() for c in df.columns]
    df = df.rename(columns={"PAYLOAD_V2": "PAYLOAD"})                     # schema drift of batch 2+
    for col in ("SOURCE_SYSTEM",):
        if col not in df:
            df[col] = None
    df = df[["PING_ID", "VEHICLE_ID", "PING_TS", "PAYLOAD", "SOURCE_SYSTEM"]]
    df["_BATCH"] = a.batch
    df["_RUN_ID"] = RUN_ID

    with sf_connect() as sf, sf.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM BRONZE.GPS_PINGS WHERE _batch = %s", (a.batch,))
        already = cur.fetchone()[0]
        if already:
            log.info("Batch %s: %s pings already loaded, resuming after them", a.batch, already)
        todo = df.iloc[already:]
        for start in range(0, len(todo), a.chunk):
            part = todo.iloc[start:start + a.chunk]
            write_pandas(sf, part, "GPS_PINGS", schema="BRONZE")
            log.info("Pushed %s pings (%s/%s)", len(part), already + start + len(part), len(df))
            time.sleep(a.sleep)


if __name__ == "__main__":
    main()
