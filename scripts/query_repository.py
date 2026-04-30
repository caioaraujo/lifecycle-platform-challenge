AUDIENCE_SEGMENTATION_QUERY = """
    WITH search_counts AS ( 
        SELECT renter_id, 
        COUNTIF(event_type = "search") as search_count 
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
        AND p.subscription_status = "churned" 
        AND s.search_count >= 3 
        AND p.phone IS NOT NULL 
        AND p.phone != "" 
        AND p.sms_consent = TRUE 
        AND NOT EXISTS ( 
            SELECT 1 
            FROM audience_segmentation.suppression_list sl 
            WHERE sl.renter_id = p.renter_id 
        ) 
        AND ( 
            p.dnd_until IS NULL 
            OR p.dnd_until < @run_timestamp 
        ) 
"""

AUDIENCE_QUERY_VALIDATION = """
    WITH daily AS (
      SELECT 
        _PARTITIONDATE,
        COUNT(*) AS daily_total
      FROM `audience_segmentation.staging.audience_segmentation_stage`
      WHERE _PARTITIONDATE BETWEEN DATE_SUB(@run_date, INTERVAL 30 DAY)
                                AND @run_date
      GROUP BY _PARTITIONDATE
    ),
    
    current AS (
      SELECT daily_total AS total_today
      FROM daily
      WHERE _PARTITIONDATE = @run_date
    ),
    
    historical AS (
      SELECT
        AVG(daily_total) AS avg,
        STDDEV(daily_total) AS stddev
      FROM daily
      WHERE _PARTITIONDATE < @run_date
    ),
    
    final AS (
      SELECT
        c.total_today,
        h.avg,
        h.stddev,
        SAFE_DIVIDE(c.total_today, h.avg) AS ratio,
        SAFE_DIVIDE(c.total_today - h.avg, h.stddev) AS z_score
      FROM current c
      CROSS JOIN historical h
    )
    
    SELECT *,
      CASE
        WHEN ratio > 2 THEN 'ANOMALY_RATIO'
        WHEN ABS(z_score) > 3 THEN 'ANOMALY_ZSCORE'
        ELSE 'OK'
      END AS status
    FROM final
"""

AUDIENCE_STAGE_QUERY = """
    SELECT
        renter_id, email, phone, last_login, search_count, days_since_login
    FROM `audience_segmentation.staging.audience_segmentation_stage`
    WHERE _PARTITIONDATE = @run_date
    """
