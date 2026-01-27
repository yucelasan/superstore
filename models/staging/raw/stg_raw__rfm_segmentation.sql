with 

source as (

    select * from {{ source('raw', 'rfm_segmentation') }}

),

renamed as (

    select
        user_id,
        recency,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        rfm_score,
        segment

    from source

)

select * from renamed