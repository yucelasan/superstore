WITH monthly AS (
  SELECT
    DATE_TRUNC(order_date, MONTH) AS month,
    category2,
    SUM(revenue) AS revenue,
    SUM(amount) AS amount
  FROM {{ ref('order_details_all') }}
  GROUP BY 1,2
),

mom AS (
  SELECT
    month,
    category2,
    revenue,
    amount,
    (revenue - LAG(revenue) OVER (PARTITION BY category2 ORDER BY month))
      / LAG(revenue) OVER (PARTITION BY category2 ORDER BY month) AS revenue_mom,
    (amount - LAG(amount) OVER (PARTITION BY category2 ORDER BY month))
      / LAG(amount) OVER (PARTITION BY category2 ORDER BY month) AS amount_mom
  FROM monthly
)

SELECT *
FROM mom