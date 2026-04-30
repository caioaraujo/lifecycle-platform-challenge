import time
import random

from scripts.client import ESPClient


def execute_campaign_send(
    campaign_id: str,
    audience: list[dict],
    esp_client: ESPClient,
    sent_log_path: str = "sent_renters.json"
) -> dict:
    """Returns {'total_sent': int, 'total_failed': int, 'total_skipped': int, 'elapsed_seconds': float}"""

    batch_size = 100
    total_sent = 0
    total_failed = 0
    total_skipped = 0
    start_time = time.time()

    for i in range(0, len(audience), batch_size):
        batch = audience[i:i + batch_size]
        max_retries = 5
        attempt = 0
        response = None
        while attempt <= max_retries:
            response = esp_client.send_batch(campaign_id, batch)
            if response.status_code != 429 or attempt == max_retries:
                break
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            time.sleep(wait_time)
            attempt += 1
        if response:
            if response.status_code == 429:
                total_failed += len(batch)
                with open(sent_log_path, "a") as sent_log:
                    sent_log.write(f"The following batch has failed to send due to rating limit: {batch}\n")
                continue
            elif response.status_code != 200:
                total_failed += len(batch)
                with open(sent_log_path, "a") as sent_log:
                    sent_log.write(f"The following batch failed to send: {batch}\n Error {response.status_code}: {response.json()}\n")
                continue
            else:
                total_sent += len(batch)
            response = response.json()
            total_skipped += response.get("total_skipped", 0)

    end_time = time.time()

    return {
        "total_sent": total_sent,
        "total_failed": total_failed,
        "total_skipped": total_skipped,
        "elapsed_seconds": end_time - start_time,
    }
