SELECT
o.order_id,
o.branch_id,
o.market_name,
b.region,
b.city,
customer.town as customer_town, 
b.branch_town as market_location,
o.order_date,
o.user_id,
o.name_surname,
customer.user_birthdate,
date_diff(o.order_date, customer.user_birthdate, year) as order_age,
date_diff("2023-08-15", customer.user_birthdate, year) AS user_age,
od.item_id,
c.category1,
c.category2,
c.category3,
c.category4,
c.item_name,
od.amount,
c.unit_price,
ROUND(od.amount * c.unit_price,2) as revenue,
o.payment_type
FROM {{ ref('int_order_details') }} as od
left join {{ ref('int_orders_market') }} as o
on od.order_id = o.order_id
left join {{ ref('stg_raw__categories') }} as c
on od.item_id = c.item_id 
left join {{ ref('stg_raw__customers') }} as customer
on customer.user_id = o.user_id
left join {{ ref('stg_raw__branches') }} as b
on o.branch_id = b.branch_id and customer.town = b.town