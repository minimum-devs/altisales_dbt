with source as (

    select
        -- From call_disposition table (aliased as cd in TS)
        id as call_disposition_id, -- Corresponds to dispositionIdField_onDispositionTable
        name as call_disposition_name -- Corresponds to dispositionNameField_onDispositionTable
    from {{ source('outreach_raw', 'call_disposition') }}

),

renamed as (

    select
        call_disposition_id,
        call_disposition_name
    from source

)

select * from renamed 