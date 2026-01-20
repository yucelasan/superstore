with 

source as (

    select * from {{ source('raw', 'categories') }}

),

renamed as (

    select
        itemid as item_id,
        category1,
        category1_id,
        category2,
        cast (category2_id as string) as category2_id,
        category3,
        cast (category3_id as string) as category3_id,
        category4,
        cast (category4_id as string) as category4_id,
        brand,
        itemcode as item_code,
        itemname as item_name,
        unit_price

    from source

)

select * from renamed