"""Source 5 - API: GBP -> EUR exchange rates (Frankfurter, ECB data, free, no key).

FreshRoute reports to its parent group in EUR, and its Paris depot is paid in EUR.
"""
import os

from common import get_logger, http_get_json, replace_payload, require_keys, sf_connect

log = get_logger("fx")
BASE = os.environ.get("FX_API_URL", "https://api.frankfurter.dev/v1")
START, END = os.environ.get("START_DATE", "2026-08-25"), os.environ.get("END_DATE", "2026-10-15")


def main():
    payload = http_get_json(f"{BASE}/{START}..{END}", params={"from": "GBP", "to": "EUR"})
    require_keys(payload, ["base", "rates"], "frankfurter")
    with sf_connect() as conn, conn.cursor() as cur:
        replace_payload(cur, "BRONZE.FX_RAW", f"GBP_EUR_{START}_{END}", payload)
    log.info("FX: %s daily rates loaded", len(payload["rates"]))


if __name__ == "__main__":
    main()
