WITH buyer_sales AS (
    SELECT customer_id, sum(total_amount) AS buyer_total
    FROM sales
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
), buyer_totals AS (
    SELECT count(*) AS eligible_buyers, sum(buyer_total) AS known_sales
    FROM buyer_sales
), all_sale_totals AS (
    SELECT sum(total_amount) AS all_sales
    FROM sales
), ranked_buyers AS (
    SELECT buyer_total, row_number() OVER (ORDER BY buyer_total DESC, customer_id) AS buyer_rank
    FROM buyer_sales
), percentages(top_percent) AS (
    VALUES (1), (5), (10)
), quotas AS (
    SELECT top_percent,
           (CAST(eligible_buyers AS HUGEINT) * top_percent + 99) // 100 AS quota
    FROM percentages
    CROSS JOIN buyer_totals
), top_totals AS (
    SELECT quotas.top_percent, count(ranked_buyers.buyer_rank) AS top_buyers,
           sum(ranked_buyers.buyer_total) AS top_sales
    FROM quotas
    LEFT JOIN ranked_buyers ON ranked_buyers.buyer_rank <= quotas.quota
    GROUP BY quotas.top_percent
)
SELECT top_totals.top_percent, buyer_totals.eligible_buyers, top_totals.top_buyers,
       top_totals.top_sales, buyer_totals.known_sales, all_sale_totals.all_sales
FROM top_totals
CROSS JOIN buyer_totals
CROSS JOIN all_sale_totals
ORDER BY top_totals.top_percent
