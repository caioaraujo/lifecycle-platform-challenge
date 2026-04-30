# Observability and Design

## What Datadog metrics and alerts would you set up for this pipeline?

For DAGs monitoring, I would set up the following Datadog metrics and alerts:

Latency:
```
avg(last_5m):airflow.dagrun.duration{dag:campaign_pipeline} > 1800
```

Dead pipeline:
```
absent(last_1h):airflow.dagrun.completed{dag:campaign_pipeline}
```

For audience size anomaly detection, I would set up the following metrics from the DAG:

```python
from datadog import statsd

statsd.gauge("audience.size", total_users)
statsd.gauge("audience.ratio_to_avg", ratio)
statsd.gauge("audience.z_score", z_score)
```
And create alerts based on these metrics, for example:

```
avg(last_5m):audience.ratio_to_avg > 2
avg(last_5m):audience.ratio_to_avg < 0.5
abs(avg(last_5m):audience.z_score) > 3

anomalies(audience.size, 'basic', 2)
```

For ESP integration, I would set up the following metrics:

```python
from datadog import statsd

statsd.increment("esp.send.success", total_sent)
statsd.increment("esp.send.failed", total_failed)
statsd.increment("esp.send.skipped", total_skipped)

statsd.gauge("esp.error_rate", total_failed / total_attempted)
statsd.gauge("esp.rate_limit_hits", rate_limit_count)
```

## How would you detect and prevent double-sends if the pipeline runs twice due to an Airflow retry or manual re-trigger?

For this scenario, I would consider to keep idempotency for the metrics in pipeline, avoiding increment and using gauge instead.
For exemple:

```python
from datadog import statsd

statsd.gauge("esp.send.success", total_sent)
statsd.gauge("esp.send.failed", total_failed)
```

Another way to prevent double-sends is tagging by the dag_run_id and execution date for grouping by execution. For example:

```python
tags = [
    f"dag_id=campaign_pipeline",
    f"run_id={context['run_id']}",
    f"execution_date={context['ds']}",
]

statsd.gauge("audience.size", total_users, tags=tags)
```

## What happens if the ESP goes down mid-send? Describe your circuit-breaker or recovery strategy

For the circuit-breaker strategy, I would implement the sending based on three states: CLOSED, OPEN and HALF-OPEN.

For example:

```python
import time

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self.failures = 0
        self.state = "CLOSED"
        self.last_failure_time = None

    def can_execute(self):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        return True

    def record_success(self):
        self.failures = 0
        self.state = "CLOSED"

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()

        if self.failures >= self.failure_threshold:
            self.state = "OPEN"

breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
```

In the sending task:

```python
for batch in batches:

    if not breaker.can_execute():
        logging.error("Circuit breaker OPEN - skipping batch")
        total_failed += len(batch)
        continue

    response = _send_with_retry(...)

    if response is None or response.status_code >= 500:
        breaker.record_failure()
        total_failed += len(batch)
        continue

    breaker.record_success()
    total_sent += len(batch)
```

For the recovery strategy, I would opt to use the retry mechanism of Airflow, raising a RuntimeError. This way, the task will be retried.

Another strategy is to persist the failed batches in a storage (ex: S3, GCS) and then retry sending them in the next execution.
