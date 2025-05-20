with source as (

    select
        -- From users table (aliased as u in TS)
        id as user_id, -- Corresponds to userIdField_onUserTable
        name as user_name -- Corresponds to userNameField_onUserTable
    from {{ source('salesloft_raw', 'users') }}

),

renamed as (

    select
        user_id,
        user_name
    from source

)

select * from renamed 