SELECT
  orders.user_id,
  orders.name_surname,
  customers.user_gender,
  count(order_id) as nb_of_orders,
  round(sum(orders.total_basket), 2) AS LTV,
  round(avg(orders.total_basket), 2) AS AOV
FROM {{ ref('int_orders') }} AS orders
LEFT JOIN {{ ref('stg_raw__customers') }} AS customers
  USING (user_id)
GROUP BY
  orders.user_id,
  orders.name_surname,
  customers.user_gender