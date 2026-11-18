SELECT strftime(event_time, '%Y-%m') AS month, kind,
       count(*) AS order_count,
       count(DISTINCT customer_id) AS customer_count,
       sum(product_line_count) AS product_line_count,
       sum(total_amount) AS total_amount,
       sum(CASE WHEN customer_id IS NOT NULL THEN total_amount ELSE 0 END) AS identified_total_amount,
       sum(CASE WHEN customer_id IS NULL THEN total_amount ELSE 0 END) AS missing_customer_total_amount
FROM orders
GROUP BY month, kind
ORDER BY month, kind;
