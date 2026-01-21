SELECT
 concat(branch_id, '-', town) as market_id,
 o.*,
 c.town
FROM {{ ref("int_orders")}} AS o
LEFT JOIN {{ref("stg_raw__customers")}} AS c
   USING(user_id)