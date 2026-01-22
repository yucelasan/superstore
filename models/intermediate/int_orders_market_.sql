select
  o.*,
  c.town AS customer_town,
  b.branch_town as market_location
from {{ ref("int_orders") }} AS o
left join {{ ref("stg_raw__customers") }} as c
using (user_id)
left join {{ ref('stg_raw__branches') }} as b
  on o.branch_id = b.branch_id
