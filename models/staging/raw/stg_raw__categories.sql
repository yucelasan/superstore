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
        CAST(category2_id AS int64) as category2_id,
        category3,
        CAST(category3_id AS int64) as category3_id,
        category4,
        CAST(category4_id AS int64) as category4_id,
        brand,
        itemcode,
        itemname

    from source

)

select * from renamed