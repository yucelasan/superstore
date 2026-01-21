WITH order_totals AS (
    SELECT
        order_id,
        ROUND(SUM(total_price),2) AS total_basket
    FROM {{ ref("int_order_details") }}
    GROUP BY order_id
)

SELECT
    o.order_id,
    o.branch_id,
    o.order_date,
    o.user_id,
    o.name_surname,
    ot.total_basket AS total_basket,
    o.payment_type,
    FROM {{ ref('stg_raw__orders') }} o
LEFT JOIN order_totals ot
    ON o.order_id = ot.order_id
