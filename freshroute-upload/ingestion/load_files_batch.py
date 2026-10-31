"""Source 1 - BATCH FILES: nightly export of the transport management system (depots, vehicles, routes).

  python ingestion/load_files_batch.py --batch 1

PUT the CSV files of one batch into an internal stage, then COPY INTO bronze (every column as text).
Replay-safe: COPY INTO remembers the files it loaded for 64 days -> a rerun loads 0 file, no duplicate.
MATCH_BY_COLUMN_NAME tolerates a renamed/added column (schema drift); METADATA$FILENAME keeps the lineage.
"""
import argparse
from pathlib import Path

from common import get_logger, sf_connect

log = get_logger("files_batch")
TABLES = ["depots", "vehicles", "routes"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, required=True)
    ap.add_argument("--data", default="data/freshroute")
    a = ap.parse_args()
    folder = Path(a.data) / f"batch_{a.batch:03d}"

    with sf_connect() as conn, conn.cursor() as cur:
        for table in TABLES:
            path = folder / f"{table}.csv"
            if not path.exists():
                raise FileNotFoundError(f"{path} missing: run tools/messy_data.py first (see README)")
            # Rename on upload so the file name carries the batch: batch_001_routes.csv
            cur.execute(f"PUT 'file://{path.resolve()}' @TMS_STAGE/{table}/batch_{a.batch:03d}/ "
                        "AUTO_COMPRESS=TRUE OVERWRITE=FALSE")
            cur.execute(f"""
                COPY INTO BRONZE.{table.upper()}
                FROM @TMS_STAGE/{table}/
                FILE_FORMAT = (FORMAT_NAME = 'CSV_RAW')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                INCLUDE_METADATA = (_SOURCE_FILE = METADATA$FILENAME, _LOADED_AT = METADATA$START_SCAN_TIME)
                ON_ERROR = 'CONTINUE'
            """)
            results = cur.fetchall()
            loaded = [r for r in results if r[1] in ("LOADED", "PARTIALLY_LOADED")]
            errors = sum(int(r[5] or 0) for r in loaded) if loaded else 0
            log.info("%s: %s file(s) loaded, %s rejected line(s) (0 file = already loaded, idempotent)",
                     table, len(loaded), errors)


if __name__ == "__main__":
    main()
