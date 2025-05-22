{% macro refresh_staging_views() %}
    {{ log("Starting refresh_staging_views macro...", info=True) }}

    {% if not execute %}
        {{ log("Skipping refresh_staging_views macro during parse phase.", info=True) }}
        {{ return(None) }}
    {% endif %}

    {{ log("Target database: " ~ target.database, info=True) }}

    {# Discover all raw schemas #}
    {%- set raw_schemas_discovery_sql -%}
        select distinct table_schema
        from {{ target.database }}.information_schema.tables
        where table_schema ilike '%_raw_%'
        order by table_schema;
    {%- endset -%}
    {{ log("DEBUG: Raw schema discovery SQL:\n" ~ raw_schemas_discovery_sql, info=True) }}
    {%- set raw_schema_results = run_query(raw_schemas_discovery_sql) -%}

    {%- set unique_raw_schemas = [] -%}
    {%- for row in raw_schema_results -%}
        {%- do unique_raw_schemas.append(row['table_schema']) -%}
    {%- endfor -%}
    {{ log("DEBUG: Unique raw schemas found: " ~ unique_raw_schemas, info=True) }}

    {% if unique_raw_schemas | length == 0 %}
        {{ log("No raw schemas found (pattern %_raw_%). Macro will exit.", info=True) }}
        {{ return(None) }}
    {% endif %}

    {%- for raw_schema_name in unique_raw_schemas -%}
        {%- set schema_parts = raw_schema_name.split('_raw_') -%}
        {%- if schema_parts | length == 2 -%}
            {%- set tenant_identifier = schema_parts[0] -%}
            {%- set provider = schema_parts[1].lower() -%} {# Ensure provider is lowercase for matching #}
            {%- set staging_schema_name = tenant_identifier ~ '_stg_' ~ provider -%}

            {{ log("Processing raw schema: " ~ raw_schema_name ~ " (Provider: " ~ provider ~ ") -> staging schema: " ~ staging_schema_name, info=True) }}

            {% set create_schema_sql = 'CREATE SCHEMA IF NOT EXISTS ' ~ adapter.quote(staging_schema_name) ~ ';' %}
            {% do run_query(create_schema_sql) %}
            {{ log("Ensured staging schema " ~ staging_schema_name ~ " exists.", info=True) }}

            {# Get all tables from the current raw_schema_name #}
            {%- set get_tables_sql -%}
                SELECT table_name 
                FROM {{ target.database }}.information_schema.tables 
                WHERE table_schema = '{{ raw_schema_name }}' AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            {%- endset -%}
            {{ log("DEBUG: Getting tables for raw schema " ~ raw_schema_name ~ " with SQL: \n" ~ get_tables_sql, info=True) }}
            {%- set raw_tables_result = run_query(get_tables_sql) -%}

            {%- for raw_table_row in raw_tables_result -%}
                {%- set table_name = raw_table_row['table_name'] -%}
                {{ log("Processing table: " ~ table_name ~ " in raw schema " ~ raw_schema_name, info=True) }}

                {%- set select_columns_list = [] -%}
                {%- set use_explicit_mapping = false -%}

                {# ----- Define Explicit Mappings START ----- #}
                {%- if provider == 'outreach' and table_name == 'call' -%}
                    {%- set use_explicit_mapping = true -%}
                    {%- set select_columns_list = [
                        {'source': 'id', 'target': 'call_id'}, 
                        {'source': 'direction', 'target': 'call_direction'}, 
                        {'source': 'created_at', 'target': 'call_timestamp'}, 
                        {'source': 'updated_at', 'target': 'call_updated_at'}, 
                        {'source': 'relationship_user_id', 'target': 'user_id'}, 
                        {'source': 'relationship_prospect_id', 'target': 'contact_id'},
                        {'source': 'outcome', 'target': 'call_outcome'}, 
                        {'source': 'note', 'target': 'call_notes'}, 
                        {'source': 'recording_url', 'target': 'call_recording_url'}, 
                        {'source': 'voicemail_recording_url', 'target': 'voicemail_url'}, 
                        {'source': 'state', 'target': 'call_status'},
                        {'expression': 'CASE WHEN completed_at IS NOT NULL AND answered_at IS NOT NULL THEN EXTRACT(EPOCH FROM (completed_at - answered_at)) ELSE NULL END', 'target': 'call_duration_seconds'}
                    ] -%}
                {%- elif provider == 'outreach' and table_name == 'users' -%}
                    {%- set use_explicit_mapping = true -%}
                    {%- set select_columns_list = [
                        {'source': 'id', 'target': 'user_id'}, {'source': 'email', 'target': 'email'}, {'source': 'first_name', 'target': 'first_name'}, {'source': 'last_name', 'target': 'last_name'},
                        {'source': 'name', 'target': 'full_name'}, {'source': 'created_at', 'target': 'user_created_at'}, {'source': 'updated_at', 'target': 'user_updated_at'},
                        {'source': 'active_prospects_count', 'target': 'active_contacts_count'}, {'source': 'title', 'target': 'job_title'}
                    ] -%}
                {%- elif provider == 'salesloft' and table_name == 'call' -%}
                    {%- set use_explicit_mapping = true -%}
                    {%- set select_columns_list = [
                        {'source': 'id', 'target': 'call_id'}, {'source': 'disposition', 'target': 'call_disposition'}, {'source': 'sentiment', 'target': 'call_sentiment'}, 
                        {'source': 'created_at', 'target': 'call_timestamp'}, {'source': 'updated_at', 'target': 'call_updated_at'}, {'source': 'user_id', 'target': 'user_id'}, 
                        {'source': 'called_person_id', 'target': 'contact_id'}, {'source': 'duration', 'target': 'call_duration_seconds'}, {'source': 'crm_activity_id', 'target': 'crm_activity_id'}
                    ] -%}
                {%- elif provider == 'salesloft' and table_name == 'users' -%}
                    {%- set use_explicit_mapping = true -%}
                    {%- set select_columns_list = [
                        {'source': 'id', 'target': 'user_id'}, {'source': 'email', 'target': 'email'}, {'source': 'first_name', 'target': 'first_name'}, {'source': 'last_name', 'target': 'last_name'},
                        {'source': 'name', 'target': 'full_name'}, {'source': 'created_at', 'target': 'user_created_at'}, {'source': 'updated_at', 'target': 'user_updated_at'},
                        {'source': 'active', 'target': 'is_active'}, {'source': 'job_role', 'target': 'job_title'}
                    ] -%}
                {%- endif -%}
                {# ----- Define Explicit Mappings END ----- #}

                {%- set final_select_clause = [] -%}
                {% if use_explicit_mapping %}
                    {{ log("Using explicit column mapping for " ~ provider ~ "." ~ table_name, info=True) }}
                    {%- for item in select_columns_list -%}
                        {%- if item.expression is defined -%}
                            {%- do final_select_clause.append(item.expression ~ ' as ' ~ adapter.quote(item.target)) -%}
                        {%- elif item.source is defined -%}
                            {%- do final_select_clause.append(adapter.quote(item.source) ~ ' as ' ~ adapter.quote(item.target)) -%}
                        {%- endif -%}
                    {%- endfor -%}
                {% else %}
                    {{ log("Using dynamic column discovery for " ~ provider ~ "." ~ table_name, info=True) }}
                    {%- set get_columns_sql -%}
                        SELECT column_name 
                        FROM {{ target.database }}.information_schema.columns 
                        WHERE table_schema = '{{ raw_schema_name }}' AND table_name = '{{ table_name }}'
                        ORDER BY ordinal_position;
                    {%- endset -%}
                    {{ log("DEBUG: Getting columns for " ~ raw_schema_name ~ "." ~ table_name ~ " with SQL: \n" ~ get_columns_sql, info=True) }}
                    {%- set columns_result = run_query(get_columns_sql) -%}
                    {%- for col_row in columns_result -%}
                        {%- set col_name = col_row['column_name'] -%}
                        {%- do final_select_clause.append(adapter.quote(col_name) ~ ' as ' ~ adapter.quote(col_name.lower().replace(' ', '_лог'))) -%}
                    {%- endfor -%}
                {% endif %}

                {% if final_select_clause | length > 0 %}
                    {%- set drop_view_sql = 'DROP VIEW IF EXISTS ' ~ adapter.quote(staging_schema_name) ~ '.' ~ adapter.quote(table_name) ~ ' CASCADE;' -%}
                    {% do run_query(drop_view_sql) %}
                    
                    {%- set create_view_sql = 'CREATE OR REPLACE VIEW ' ~ adapter.quote(staging_schema_name) ~ '.' ~ adapter.quote(table_name) ~ ' AS SELECT ' ~ final_select_clause | join(', ') ~ ' FROM ' ~ adapter.quote(raw_schema_name) ~ '.' ~ adapter.quote(table_name) ~ ';' -%}
                    {% do run_query(create_view_sql) %}
                    {{ log("Created/Replaced view " ~ staging_schema_name ~ "." ~ table_name, info=True) }}
                {% else %}
                    {{ log("No columns to select for " ~ raw_schema_name ~ "." ~ table_name ~ ". Skipping view creation.", warning=True) }}
                {% endif %}
            {%- endfor -%}
        {%- else -%}
            {{ log("Could not derive tenant/provider from raw schema: " ~ raw_schema_name, warning=True) }}
        {%- endif -%}
    {%- endfor -%}

    {{ log("Finished refresh_staging_views macro.", info=True) }}

    {% if execute %}
        {{ log("Attempting explicit COMMIT at end of refresh_staging_views", info=True) }}
        {% do run_query("COMMIT;") %}
        {{ log("Explicit COMMIT executed.", info=True) }}
    {% endif %}
{% endmacro %} 