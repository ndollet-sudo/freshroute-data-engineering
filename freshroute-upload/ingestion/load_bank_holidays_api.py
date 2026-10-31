"""Source 6 - API: UK bank holidays (official GOV.UK JSON, free, no key).

One payload per division: england-and-wales, scotland, northern-ireland (Belfast depot).
"""
from common import get_logger, http_get_json, replace_payload, require_keys, sf_connect

log = get_logger("bank_holidays")
URL = "https://www.gov.uk/bank-holidays.json"


def main():
    payload = http_get_json(URL)
    require_keys(payload, ["england-and-wales", "scotland", "northern-ireland"], "gov.uk")
    with sf_connect() as conn, conn.cursor() as cur:
        for division, content in payload.items():
            require_keys(content, ["events"], f"gov.uk.{division}")
            replace_payload(cur, "BRONZE.BANK_HOLIDAYS_RAW", division, content)
            log.info("%s: %s bank holidays", division, len(content["events"]))


if __name__ == "__main__":
    main()
