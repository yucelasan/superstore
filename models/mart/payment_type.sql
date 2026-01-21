SELECT
  payment_type,
  COUNT(order_id) AS order_count,
  round(sum(total_basket), 2) AS total_value
FROM {{ ref('int_orders_market') }}
GROUP BY payment_type
