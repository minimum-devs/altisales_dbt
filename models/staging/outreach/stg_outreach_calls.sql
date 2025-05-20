with source as (

    select
        -- From call table (aliased as c in TS)
        id as call_id, -- Corresponds to callIdField
        created_at as call_created_at, -- Corresponds to callCreatedAtField
        relationship_user_id as user_id, -- Corresponds to callUserIdField_onCallTable
        relationship_call_disposition_id as call_disposition_id -- Corresponds to callRelationshipToDispositionIdField_onCallTable
    from {{ source('outreach_raw', 'call') }}

),

renamed as (

    select
        call_id,
        call_created_at,
        user_id,
        call_disposition_id
    from source

)

select * from renamed 