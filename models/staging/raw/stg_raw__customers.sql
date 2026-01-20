with 

source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        userid,
        username_ as username,
        namesurname,
        usergender,
        userbirthdate,
        region,
        city,
        town,
        district,
        addresstext

    from source

)

select * from renamed