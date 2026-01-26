with market_count as (
SELECT
  region,
  city,
  COUNT(DISTINCT market_name) as nb_of_market
FROM {{ ref('order_details_all') }}
GROUP BY region, city order by 3 )
SELECT
  od.region,
  od.city,
  m.nb_of_market,
  od.market_name,
  count(distinct customer_town) as nb_0f_customer_town,
  COUNT(DISTINCT order_id) as nb_of_order,
  round (sum(revenue),2) as sum_of_revenue
FROM {{ ref('order_details_all') }} as od
left join market_count as m on m.city = od.city 
GROUP BY od.region, od.city,  m.nb_of_market, od.market_name
order by sum(revenue)
