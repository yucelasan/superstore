with 

source as (

    select * from {{ source('raw', 'order_details') }}

),

renamed as (

    select
        orderid as order_id,
        orderdetailid as order_detail_id,
        amount,
        cast ( unitprice as float64) as unit_price,
        cast ( totalprice as float64) as total_price,
        itemid as item_id,
        itemcode as item_code

    from source

)

select * from renamed