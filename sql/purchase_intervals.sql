WITH purchase_times AS (
    SELECT DISTINCT customer_id, event_time
    FROM sales
    WHERE customer_id IS NOT NULL
), ordered_times AS (
    SELECT customer_id, event_time,
           lag(event_time) OVER customer_history AS previous_time,
           row_number() OVER customer_history AS purchase_number
    FROM purchase_times
    WINDOW customer_history AS (PARTITION BY customer_id ORDER BY event_time)
), gaps AS (
    SELECT customer_id,
           CASE WHEN purchase_number = 2 THEN 1 ELSE 2 END AS segment_index,
           epoch_us(event_time) - epoch_us(previous_time) AS gap_microseconds
    FROM ordered_times
    WHERE previous_time IS NOT NULL
), segments(segment_index, segment) AS (
    VALUES (1, 'first'), (2, 'later')
), buckets(lower_microseconds, upper_microseconds) AS (
    -- Half-open ranges in elapsed microseconds, with cutoffs at 1, 7, 30 and 90 days.
    VALUES (0, 86400000000),
           (86400000000, 604800000000),
           (604800000000, 2592000000000),
           (2592000000000, 7776000000000),
           (7776000000000, CAST(NULL AS BIGINT))
), segment_stats AS (
    SELECT segment_index, count(*) AS interval_count, count(DISTINCT customer_id) AS customer_count,
           min(gap_microseconds) AS min_gap_microseconds, max(gap_microseconds) AS max_gap_microseconds
    FROM gaps
    GROUP BY segment_index
), bucket_stats AS (
    SELECT gaps.segment_index, buckets.lower_microseconds, count(*) AS interval_count
    FROM gaps
    JOIN buckets ON gaps.gap_microseconds >= buckets.lower_microseconds
                 AND (gaps.gap_microseconds < buckets.upper_microseconds OR buckets.upper_microseconds IS NULL)
    GROUP BY gaps.segment_index, buckets.lower_microseconds
)
SELECT segments.segment, coalesce(segment_stats.interval_count, 0) AS interval_count,
       coalesce(segment_stats.customer_count, 0) AS customer_count,
       segment_stats.min_gap_microseconds, segment_stats.max_gap_microseconds,
       buckets.lower_microseconds, buckets.upper_microseconds,
       coalesce(bucket_stats.interval_count, 0) AS bucket_interval_count
FROM segments
CROSS JOIN buckets
LEFT JOIN segment_stats ON segment_stats.segment_index = segments.segment_index
LEFT JOIN bucket_stats ON bucket_stats.segment_index = segments.segment_index
                      AND bucket_stats.lower_microseconds = buckets.lower_microseconds
ORDER BY segments.segment_index, buckets.lower_microseconds
