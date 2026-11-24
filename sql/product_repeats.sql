WITH observed_products AS (
    -- Each input row represents one distinct product in one identified positive order.
    SELECT order_key, customer_id, event_time,
           min(event_time) OVER (PARTITION BY customer_id) AS first_purchase,
           min(event_time) OVER (PARTITION BY customer_id, stock_code) AS first_product_purchase
    FROM order_products
)
SELECT order_key, customer_id, replace(CAST(event_time AS VARCHAR), ' ', 'T') AS event_time,
       first_purchase < event_time AS has_prior_purchase,
       count(*) AS distinct_products,
       count(*) FILTER (WHERE first_product_purchase < event_time) AS repeated_products
FROM observed_products
GROUP BY order_key, customer_id, event_time, first_purchase
ORDER BY order_key
