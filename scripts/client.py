class Response:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.body = body

    def json(self):
        return self.body


class ESPClient:
    def send_batch(self, campaign_id: str, recipients: list[dict]) -> Response:
        """Sends a batch of recipients to the ESP.
        Returns a Response with .status_code and .json()"""
        result = {
            "total_sent": 96,
            "total_failed": 3,
            "total_skipped": 1,
            "elapsed_seconds": 0.45
        }
        return Response(200, result)
