# Value Model Integration

## Query evolution

For the query evolution, I consider to query the latest scores to avoid using of old scores and duplicated user in result:

```sql
WITH search_counts AS (
  SELECT
    renter_id,
    COUNTIF(event_type = 'search') as search_count
  FROM `audience_segmentation.renter_activity`
  WHERE event_timestamp >= TIMESTAMP_SUB(@run_timestamp, INTERVAL 90 DAY)
  GROUP BY renter_id
),

latest_scores AS (
  SELECT
    renter_id,
    predicted_conversion_probability,
    model_version,
    scored_at
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY renter_id
             ORDER BY scored_at DESC
           ) AS rn
    FROM `ml_predictions.renter_send_scores`
    WHERE scored_at >= TIMESTAMP_SUB(@run_timestamp, INTERVAL 7 DAY)
  )
  WHERE rn = 1
)

SELECT
  p.renter_id,
  p.email,
  p.phone,
  p.last_login,
  s.search_count,
  DATE_DIFF(@run_date, DATE(p.last_login), DAY) AS days_since_login,
  ls.predicted_conversion_probability,
  ls.model_version,

  CASE
    WHEN ls.predicted_conversion_probability >= 0.7 THEN 'HIGH'
    WHEN ls.predicted_conversion_probability >= 0.4 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS score_bucket

FROM `audience_segmentation.renter_profiles` p

JOIN search_counts s
  ON p.renter_id = s.renter_id

LEFT JOIN latest_scores ls
  ON p.renter_id = ls.renter_id

WHERE
  p.last_login < TIMESTAMP_SUB(@run_timestamp, INTERVAL 30 DAY)
  AND p.subscription_status = 'churned'
  AND s.search_count >= 3
  AND p.phone IS NOT NULL
  AND p.phone != ''
  AND p.sms_consent = TRUE

  AND NOT EXISTS (
    SELECT 1
    FROM `audience_segmentation.suppression_list` sl
    WHERE sl.renter_id = p.renter_id
  )

  AND (
    p.dnd_until IS NULL
    OR p.dnd_until < @run_timestamp
  )
```

Note that I'm using score bucket to categorize the predicted conversion probability into HIGH, MEDIUM and LOW, 
which can be used for better targeting and prioritization of the audience for the marketing campaign.

## DAG evolution

For the Airflow DAG, I should condider to add a task which validates the scores for the day, assuming the freshness in the query:

```sql
WHERE DATE(scored_at) = @run_date
  AND scored_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
```

It avoids old scores to be used.

I would also add a validation for the total, in case of the model scores coverage is low. 
