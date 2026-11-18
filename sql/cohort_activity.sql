WITH observation AS (
    SELECT CAST(? AS TIMESTAMP) AS observed_from, CAST(? AS TIMESTAMP) AS observed_until
), customer_cohorts AS (
    SELECT customer_id, date_trunc('month', min(event_time))::TIMESTAMP AS cohort_month
    FROM sales
    GROUP BY customer_id
), cohort_sizes AS (
    SELECT cohort_month, count(*) AS cohort_customers
    FROM customer_cohorts
    GROUP BY cohort_month
), monthly_activity AS (
    SELECT cohort_month, date_trunc('month', event_time)::TIMESTAMP AS activity_month,
           count(DISTINCT sales.customer_id) AS active_customers
    FROM sales
    JOIN customer_cohorts USING (customer_id)
    GROUP BY cohort_month, activity_month
), calendar AS (
    SELECT cohort_month, cohort_customers, activity_month,
           activity_month >= observed_from
               AND activity_month + INTERVAL '1 month' <= observed_until AS complete
    FROM cohort_sizes
    CROSS JOIN observation
    CROSS JOIN LATERAL range(cohort_month, observed_until, INTERVAL '1 month') AS months(activity_month)
)
SELECT strftime(calendar.cohort_month, '%Y-%m') AS cohort_month,
       date_diff('month', calendar.cohort_month, calendar.activity_month) AS age_months,
       strftime(calendar.activity_month, '%Y-%m') AS activity_month,
       cohort_customers,
       CASE WHEN complete THEN 'complete' ELSE 'censored' END AS status,
       CASE WHEN complete THEN coalesce(active_customers, 0) END AS active_customers
FROM calendar
LEFT JOIN monthly_activity USING (cohort_month, activity_month)
ORDER BY calendar.cohort_month, calendar.activity_month
