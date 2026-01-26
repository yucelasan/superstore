WITH quarterly AS (
  SELECT
    DATE_TRUNC(order_date, QUARTER) AS quarter,
    SUM(revenue) AS revenue,
    SUM(amount) AS amount
  FROM {{ ref('order_details_all') }}
  GROUP BY 1
),

qoq AS (
  SELECT
    quarter,
    revenue,
    amount,

    -- QoQ Revenue Growth (oran)
    SAFE_DIVIDE(
      revenue - LAG(revenue) OVER (ORDER BY quarter),
      LAG(revenue) OVER (ORDER BY quarter)
    ) AS revenue_qoq,

    -- QoQ Amount Growth (oran)
    SAFE_DIVIDE(
      amount - LAG(amount) OVER (ORDER BY quarter),
      LAG(amount) OVER (ORDER BY quarter)
    ) AS amount_qoq

  FROM quarterly
)

SELECT
  quarter,
  revenue,
  amount,
  ROUND(revenue_qoq * 100, 2) AS revenue_qoq_pct,
  ROUND(amount_qoq * 100, 2) AS amount_qoq_pct
FROM qoq
ORDER BY quarter;