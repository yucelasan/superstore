SELECT user_id,
       MIN(order_date) AS first_order_date,
       MAX(order_date) AS last_order_date,
       date_diff(MAX(order_date), MIN(order_date), day) AS order_date_diff,
       date_diff("2023-08-15", MIN(order_date), day) as lifetime_days,
       date_diff("2023-08-15", MAX(order_date), day) as inactive_days 
FROM {{ref ('int_orders_market')}}
GROUP BY user_id