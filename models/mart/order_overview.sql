SELECT
  order_id,
  ANY_VALUE(order_date) AS order_date,
  ANY_VALUE(branch_id) AS branch_id,
  ANY_VALUE(market_name) AS market_name,
  ANY_VALUE(region) AS region,
  ANY_VALUE(city) AS city,
  ANY_VALUE(customer_town) AS customer_town,
  ANY_VALUE(market_location) AS market_location,
  ANY_VALUE(user_id) AS user_id,
  ANY_VALUE(name_surname) AS name_surname,
  ANY_VALUE(user_age) AS user_age,
  ANY_VALUE(order_age) AS order_age,
  ANY_VALUE(payment_type) AS payment_type,
  COUNT(DISTINCT item_id) AS product_count,
  SUM(amount) AS total_quantity,
  ROUND(SUM(amount * unit_price), 2) AS total_revenue

FROM {{ ref('order_details_all') }}
GROUP BY order_id