with order_level as (
    select
        user_id,
        order_id,
        order_date,
        total_basket
    from {{ ref('int_orders') }}
),

customer_agg as (
    select
        user_id,
        count(distinct order_id) as nb_of_orders,
        round(sum(total_basket), 2) as ltv,
        round(avg(total_basket), 2) as aov,
        min(total_basket) as min_order_amount,
        max(total_basket) as max_order_amount,

        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from order_level
    group by user_id
)

select
    user_id,
    nb_of_orders,
    ltv,
    aov,
    min_order_amount,
    max_order_amount,

    -- lifetime
    date_diff(last_order_date, first_order_date, day) as lifetime_days,

    -- inactivity
    date_diff(date '2023-08-15', last_order_date, day) as inactive_days,

    -- order spacing
    safe_divide(
        date_diff(last_order_date, first_order_date, day),
        nullif(nb_of_orders - 1, 0)
    ) as order_date_diff

from customer_agg

