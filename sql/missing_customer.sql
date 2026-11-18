SELECT strftime(event_time, '%Y-%m') AS month, kind,
       count(*) AS order_count,
       count(*) FILTER (WHERE customer_id IS NULL) AS missing_customer_order_count,
       sum(product_line_count) AS product_line_count,
       sum(CASE WHEN customer_id IS NULL THEN product_line_count ELSE 0 END) AS missing_customer_line_count,
       sum(total_amount) AS total_amount,
       sum(CASE WHEN customer_id IS NULL THEN total_amount ELSE 0 END) AS missing_customer_total_amount
FROM orders
GROUP BY month, kind
ORDER BY month, kind;
