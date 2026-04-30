import logging
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

    logging.info("Starting campaign sending")
    for i in range(0, len(audience), batch_size):
        batch = audience[i:i + batch_size] # limit data by batches to avoid rate limit error
        max_retries = 5
        attempt = 0
        response = None

        # Add retries in case of rate limit
        while attempt < max_retries:
            logging.info(f"Sending batch of size {len(batch)} in the {attempt} attempt")
            try:
                response = esp_client.send_batch(campaign_id, batch)
            except Exception as e:
                logging.error("Error calling ESP")
                attempt += 1
                time.sleep(2 ** attempt)
                continue
            if response.status_code == 429:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait_time)
                attempt += 1
                logging.warning(f"Rate limit error in the {attempt} attempt. Trying again in {wait_time} seconds")

        if response.status_code == 429:
            total_failed += len(batch)
            logging.warning(f"Batch failed to send due to rating limit after {max_retries} attempts")
            _append_log(
                sent_log_path,
                f"The following batch has failed to send due to rating limit: {batch}\n")
            continue

        if response.status_code != 200:
            total_failed += len(batch)
            logging.warning(f"Batch failed to send due to an error with status code {response.status_code} after {max_retries} attempts")
            _append_log(
                sent_log_path,
                f"The following batch failed to send: {batch}\n Error {response.status_code}: {response.json()}\n")
            continue

        total_sent += len(batch)
        logging.info(f"Batch sent successfully")
        _append_log(
            sent_log_path,
            f"The following batch was sent successfully: {batch}\n")
        response = response.json()
        total_skipped += response.get("total_skipped", 0)

    end_time = time.time()
    elapsed_time = end_time - start_time

    return {
        "total_sent": total_sent,
        "total_failed": total_failed,
        "total_skipped": total_skipped,
        "elapsed_seconds": f"{elapsed_time:.6f}",
    }


def _append_log(log_path: str, log_text: str):
    with open(log_path, "a") as log:
        log.write(log_text)
