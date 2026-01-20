SELECT *
FROM {{ ref('stg_raw__orders') }}
WHERE order_date > CURRENT_DATE()
   OR order_date < DATE '2000-01-01'
