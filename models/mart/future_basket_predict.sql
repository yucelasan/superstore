with base as (

    select
        order_id,
        user_id,
        order_date,
        total_basket,
        branch_id,
        payment_type
    from {{ ref('stg_raw__orders') }}

),

features as (

    select
        *,
        -- geçmiş sepet bilgileri
        lag(total_basket) over (partition by user_id order by order_date) as last_basket,

        avg(total_basket) over (
            partition by user_id
            order by order_date
            rows between unbounded preceding and 1 preceding
        ) as avg_basket_so_far,

        count(*) over (
            partition by user_id
            order by order_date
            rows between unbounded preceding and 1 preceding
        ) as orders_count_so_far,

        -- zaman farkı
        date_diff(
            order_date,
            lag(order_date) over (partition by user_id order by order_date),
            day
        ) as days_since_last_order,

        -- TARGET
        lead(total_basket) over (
            partition by user_id
            order by order_date
        ) as next_basket

    from base
)

select *
from features
where next_basket is not null
