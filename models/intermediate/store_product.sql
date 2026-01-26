with table as (SELECT
o.order_id,
o.branch_id,
o.order_date,
o.user_id,
o.name_surname,
od.item_id,
od.amount,
ROUND(od.amount * c.unit_price,2) as revenue,
c.item_name,
c.unit_price
FROM {{ ref('int_orders')}} as o
left join {{ref('int_order_details')}} as od
on od.order_id = o.order_id
left join {{ref('stg_raw__categories')}} as c
on od.item_id = c.item_id)

select sum(revenue) as total_order_revenue,user_id,order_id,
min(sum(revenue)) over(partition by user_id) as min_order_amount,
max(sum(revenue)) over(partition by user_id) as max_order_amount
from table
group by order_id,user_id
