with 

source as (

    select * from {{ source('raw', 'branches') }}

),

renamed as (

    select
        branch_id,
        region,
        city,
        town,
        branch_town,
        lat,
        lon

    from source

)

select * from renamed