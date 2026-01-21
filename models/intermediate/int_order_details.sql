WITH detail AS (
    SELECT
    od.order_id,
    od.order_detail_id,
    od.amount,
    c.unit_price,
    od.total_price,
    od.item_id,
    od.item_code
FROM {{ ref('stg_raw__order_details') }} od
LEFT JOIN {{ ref('stg_raw__categories') }} c
    ON od.item_id = c.item_id
)

SELECT 
order_id,
order_detail_id,
amount,
unit_price,
ROUND((amount * unit_price), 2) as total_price,
item_id,
item_code
FROM detail 


