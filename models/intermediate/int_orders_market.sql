select 
o.*, 
concat('3A', '-', b.city, '-', b.branch_town) as market_name,
b.region,
b.city,
c.town as customer_town, 
b.branch_town as market_location
from {{ ref("int_orders") }} as o
left join {{ ref("stg_raw__customers") }} as c on o.user_id = c.user_id
left join
    {{ ref("stg_raw__branches") }} as b on o.branch_id = b.branch_id and c.town = b.town
