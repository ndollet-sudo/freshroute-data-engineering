"""Source 4 - API: daily weather at every depot (Open-Meteo archive, free, no key).

One JSON payload per depot city is stored as VARIANT in BRONZE.WEATHER_RAW; dbt flattens it in silver.
"""
import csv
import os

from common import get_logger, http_get_json, replace_payload, require_keys, sf_connect

log = get_logger("weather")
URL = os.environ.get("WEATHER_API_URL", "https://archive-api.open-meteo.com/v1/archive")
START, END = os.environ.get("START_DATE", "2026-08-25"), os.environ.get("END_DATE", "2026-10-15")


def main():
    with open("dbt/freshroute/seeds/depot_cities.csv", encoding="utf-8") as f:
        cities = list(csv.DictReader(f))

    with sf_connect() as conn, conn.cursor() as cur:
        for c in cities:
            payload = http_get_json(URL, params={
                "latitude": c["latitude"], "longitude": c["longitude"],
                "start_date": START, "end_date": END,
                "daily": "precipitation_sum,temperature_2m_max,wind_speed_10m_max",
                "timezone": "Europe/London",
            })
            require_keys(payload, ["daily"], "open-meteo")
            require_keys(payload["daily"], ["time", "precipitation_sum"], "open-meteo.daily")
            replace_payload(cur, "BRONZE.WEATHER_RAW", c["city"], payload)
            log.info("%s: %s days loaded", c["city"], len(payload["daily"]["time"]))


if __name__ == "__main__":
    main()
