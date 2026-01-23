with 

source as (

    select * from {{ source('raw', 'orders') }}

),

renamed as (

    select
        orderid as order_id,
        branch_id,
        CAST(date_ AS DATE) AS order_date,
        userid AS user_id,
        namesurname AS name_surname,
        CAST(totalbasket AS FLOAT64) AS total_basket,
        payment_type

    from source

)

select * from renamed
