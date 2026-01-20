SELECT *
FROM {{ source('raw', 'orders') }}
WHERE TOTALBASKET <= 0
   OR TOTALBASKET IS NULL


