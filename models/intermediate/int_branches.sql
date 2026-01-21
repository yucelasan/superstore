SELECT
 concat(branch_id, '-', town) as market_id,
 region,
 city,
 town,
 branch_town,
 lat,
 lon
FROM {{ ref('stg_raw__branches')}}

