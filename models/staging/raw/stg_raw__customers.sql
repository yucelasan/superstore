with 

source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        userid as user_id ,
        username_ as username,
        namesurname as name_surname,
        usergender as user_gender,
        userbirthdate as user_birthdate,
        region,
        city,
        town,
        district,
        addresstext as address

    from source

)

select * from renamed