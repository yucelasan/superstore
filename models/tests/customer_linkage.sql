SELECT o.*
FROM {{ ref('stg_raw__orders') }} o
LEFT JOIN {{ ref('stg_raw__customers') }} c
  ON o.user_id = c.user_id
WHERE c.user_id IS NULL
 