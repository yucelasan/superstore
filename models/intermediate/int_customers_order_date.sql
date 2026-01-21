SELECT user_id,
       MIN(order_date) AS first_order_date,
       MAX(order_date) AS last_order_date,
       date_diff("2023-08-15", MIN(order_date), day) as LTV
FROM {{ref ('int_orders_market')}}
GROUP BY user_id