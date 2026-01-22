SELECT 
orders.order_id,
orders.user_id,
orders.order_date,
od.item_id,
c.category1,
c.category2,
c.category3,
c.category4,
c.unit_price,
customer.user_birthdate,
date_diff(orders.order_date, customer.user_birthdate, year) as order_age,
date_diff("2023-08-15", customer.user_birthdate, year) AS user_age
FROM
  {{ref("int_orders_market")}} AS orders
LEFT JOIN {{ref("stg_raw__customers")}} as customer
  USING (user_id)
left JOIN {{ref("int_order_details")}} as od
on od.order_id = orders.order_id
left join {{ref("stg_raw__categories")}} as c
on od.item_id = c.item_id