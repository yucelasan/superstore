with 

source as (

    select * from {{ source('raw', 'categories') }}

),

renamed as (

    select
        itemid,
        category1,
        category1_id,
        category2,
        cast(category2_id as int64) as category2_id,
        category3,
        cast(category3_id as int64) as category3_id,
        category4,
        cast(category4_id as int64) as category4_id,
        brand,
        itemcode,
        itemname,
        unit_price

    from source

)

select * from renamed