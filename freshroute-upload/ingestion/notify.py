"""Alert: posts a message to a Slack / Teams incoming webhook (SLACK_WEBHOOK_URL). Without a webhook, prints it.

  python ingestion/notify.py "FreshRoute pipeline FAILED at step dbt_build"
"""
import os
import sys

import requests


def main():
    message = " ".join(sys.argv[1:]) or "FreshRoute pipeline: no message"
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        print(f"[ALERT - no webhook configured] {message}")
        return
    requests.post(url, json={"text": f":rotating_light: {message}"}, timeout=10).raise_for_status()


if __name__ == "__main__":
    main()
