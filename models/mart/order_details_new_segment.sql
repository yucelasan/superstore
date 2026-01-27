SELECT
 o.*,
 s.Recency,
 s.Frequency,
 s.Monetary,
 s.R_score,
 s.F_score,
 s.M_score,
 s.RFM_score,
 s.segment
FROM {{ ref('order_details_all') }} AS o 
LEFT JOIN {{ ref('stg_raw__rfm_segmentation') }} AS s 
ON o.user_id = s.user_id