with source as (

    select
        -- From call table (aliased as c in TS)
        id as call_id, -- Corresponds to callIdField
        created_at as call_created_at, -- Corresponds to callCreatedAtField
        disposition as call_disposition_name, -- Corresponds to callDispositionNameField_onCallTable
        user_id -- Corresponds to callUserIdField_onCallTable
    from {{ source('salesloft_raw', 'call') }}

),

renamed as (

    select
        call_id,
        call_created_at,
        call_disposition_name,
        user_id
    from source

)

select * from renamed 