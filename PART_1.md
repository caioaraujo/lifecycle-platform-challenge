# Audience Segmentation Query (SQL)

```sql
    WITH search_counts AS (
      SELECT
        renter_id,
        COUNTIF(event_type = 'search') as search_count
      FROM audience_segmentation.renter_activity
      WHERE event_timestamp >= TIMESTAMP_SUB(@run_timestamp, INTERVAL 90 DAY)
      GROUP BY renter_id
    )
    
    SELECT
      p.renter_id,
      p.email,
      p.phone,
      p.last_login,
      s.search_count,
      DATE_DIFF(@run_date, DATE(p.last_login), DAY) AS days_since_login
    
    FROM audience_segmentation.renter_profiles p
    JOIN search_counts s
      ON p.renter_id = s.renter_id
    
    WHERE
      p.last_login < TIMESTAMP_SUB(@run_timestamp, INTERVAL 30 DAY)
      AND p.subscription_status = 'churned'
      AND s.search_count >= 3
      AND p.phone IS NOT NULL
      AND p.phone != ''
      AND p.sms_consent = TRUE
    
      AND NOT EXISTS (
        SELECT 1
        FROM audience_segmentation.suppression_list sl
        WHERE sl.renter_id = p.renter_id
      )
    
      AND (
        p.dnd_until IS NULL
        OR p.dnd_until < @run_timestamp
      );
```
