select
    customers.user_id,
    customers.name_surname,
    customers.user_gender,
    count(order_id) as nb_of_orders,
    ifnull(round(sum(orders.total_basket), 2),0) as ltv,
    ifnull(round(avg(orders.total_basket), 2),0) as aov,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    ifnull(date_diff(max(order_date), min(order_date), day),0) as order_date_diff,
    ifnull(date_diff("2023-08-15", min(order_date), day),0) as lifetime_days,
    ifnull(date_diff("2023-08-15", max(order_date), day),0) as inactive_days
from {{ ref('stg_raw__customers') }} as customers
left join {{ ref('int_orders_market') }} as orders  using (user_id) 
group by customers.user_id, customers.name_surname, customers.user_gender