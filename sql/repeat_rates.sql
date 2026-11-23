WITH observation AS (
    SELECT CAST(? AS TIMESTAMP) AS observed_until
), first_sales AS (
    SELECT customer_id, min(event_time) AS first_sale
    FROM sales
    GROUP BY customer_id
), first_repeats AS (
    SELECT first_sales.customer_id, first_sale, min(event_time) AS repeat_at
    FROM first_sales
    LEFT JOIN sales ON sales.customer_id = first_sales.customer_id AND sales.event_time > first_sale
    GROUP BY first_sales.customer_id, first_sale
), horizons(horizon_days) AS (
    VALUES (30), (60), (90)
), customer_windows AS (
    SELECT horizon_days, repeat_at, first_sale + horizon_days * INTERVAL '1 day' AS horizon_end
    FROM horizons
    LEFT JOIN first_repeats ON true
)
SELECT horizon_days,
       count(horizon_end) AS total_customers,
       count(*) FILTER (WHERE horizon_end <= observed_until) AS eligible_customers,
       count(*) FILTER (WHERE horizon_end > observed_until) AS censored_customers,
       count(*) FILTER (WHERE horizon_end <= observed_until AND repeat_at < horizon_end) AS repeat_customers
FROM customer_windows
CROSS JOIN observation
GROUP BY horizon_days
ORDER BY horizon_days
