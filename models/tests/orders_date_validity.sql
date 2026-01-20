SELECT *
FROM {{ ref('stg_raw__orders') }}
WHERE order_date > CURRENT_TIMESTAMP()
   OR order_date < TIMESTAMP '2000-01-01'
