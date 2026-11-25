SELECT month, kind, customer_known,
       count(*) AS order_count,
       sum(retained_lines) AS retained_lines,
       CAST(sum(retained_units) AS VARCHAR) AS retained_units,
       sum(retained_amount) AS retained_amount,
       sum(collapsed_lines) AS collapsed_lines,
       CAST(sum(collapsed_units) AS VARCHAR) AS collapsed_units,
       sum(collapsed_amount) AS collapsed_amount
FROM scenario_orders
GROUP BY month, kind, customer_known
ORDER BY month, kind, customer_known;
