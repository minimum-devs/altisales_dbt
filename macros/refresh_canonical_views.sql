{% macro refresh_canonical_views() %}
    {{ log("Starting refresh_canonical_views macro...", info=True) }}

    {% if not execute %}
        {{ log("Skipping refresh_canonical_views macro during parse phase.", info=True) }}
        {{ return(None) }}
    {% endif %}

    {# Discover staging schemas #}
    {%- set staging_schemas_sql -%}
        select distinct table_schema
        from {{ target.database }}.information_schema.views
        where table_schema ilike '%_stg_%'
        order by table_schema;
    {%- endset -%}
    {{ log("RefreshCanonicalViews: Staging schema discovery SQL: " ~ staging_schemas_sql, info=True) }}
    {%- set staging_schema_results = run_query(staging_schemas_sql) -%}

    {%- set unique_staging_schemas = [] -%}
    {%- for row in staging_schema_results -%}
        {%- do unique_staging_schemas.append(row['table_schema']) -%}
    {%- endfor -%}
    {{ log("RefreshCanonicalViews: Found staging schemas: " ~ unique_staging_schemas, info=True) }}

    {% if unique_staging_schemas | length == 0 %}
        {{ log("RefreshCanonicalViews: No staging schemas found (pattern %_stg_%). Macro will exit.", info=True) }}
        {{ return(None) }}
    {% endif %}

    {%- for staging_schema_name in unique_staging_schemas -%}
        {%- set tenant_parts = staging_schema_name.split('_stg_') -%}
        {%- if tenant_parts | length > 0 -%}
            {%- set tenant_identifier = tenant_parts[0] -%}
            {%- set canonical_schema_name = tenant_identifier ~ '_canonical' -%}

            {{ log("RefreshCanonicalViews: Processing staging schema: " ~ staging_schema_name ~ " -> canonical schema: " ~ canonical_schema_name, info=True) }}

            {%- set create_schema_sql = 'CREATE SCHEMA IF NOT EXISTS ' ~ adapter.quote(canonical_schema_name) ~ ';' -%}
            {{ log("RefreshCanonicalViews: Executing SQL: " ~ create_schema_sql, info=True) }}
            {% do run_query(create_schema_sql) %}
            {{ log("RefreshCanonicalViews: SQL execution for schema creation complete for " ~ canonical_schema_name, info=True) }}

            {# Get all views from the current staging_schema_name #}
            {%- set get_views_sql -%}
                SELECT table_name 
                FROM {{ target.database }}.information_schema.views 
                WHERE table_schema = '{{ staging_schema_name }}'
                ORDER BY table_name;
            {%- endset -%}
            {{ log("DEBUG: Getting views for staging schema " ~ staging_schema_name ~ " with SQL: \n" ~ get_views_sql, info=True) }}
            {%- set staging_views_result = run_query(get_views_sql) -%}

            {%- for staging_view_row in staging_views_result -%}
                {%- set view_name = staging_view_row['table_name'] -%}
                {{ log("RefreshCanonicalViews: Processing view: " ~ view_name ~ " in staging schema " ~ staging_schema_name, info=True) }}

                {%- set target_canonical_view_fqn = canonical_schema_name ~ '.' ~ view_name -%}
                
                {%- set drop_view_sql = 'DROP VIEW IF EXISTS ' ~ adapter.quote(canonical_schema_name) ~ '.' ~ adapter.quote(view_name) ~ ' CASCADE;' -%}
                {% do run_query(drop_view_sql) %}

                {%- set create_view_sql = 'CREATE OR REPLACE VIEW ' ~ adapter.quote(canonical_schema_name) ~ '.' ~ adapter.quote(view_name) ~ ' AS SELECT * FROM ' ~ adapter.quote(staging_schema_name) ~ '.' ~ adapter.quote(view_name) ~ ';' -%}
                {{ log("RefreshCanonicalViews: Attempting to create/replace canonical view " ~ target_canonical_view_fqn ~ " with SQL: " ~ create_view_sql, info=True) }}
                {% do run_query(create_view_sql) %}
                {{ log("RefreshCanonicalViews: SQL execution for view creation/replacement complete for " ~ target_canonical_view_fqn, info=True) }}
            {%- endfor -%}

            {# Verify views in schema at the end of the loop #}
            {%- set verify_views_sql = "SELECT table_schema, table_name FROM information_schema.views WHERE table_schema = '" ~ canonical_schema_name ~ "';" -%}
            {{ log("RefreshCanonicalViews: Verifying views in schema " ~ canonical_schema_name ~ " with SQL: " ~ verify_views_sql, info=True) }}
            {%- set verify_views_results = run_query(verify_views_sql) -%}
            {{ log("📋 RefreshCanonicalViews: Views found in schema " ~ canonical_schema_name ~ " after all attempts: " ~ verify_views_results.rows, info=True) }}

        {%- else -%}
            {{ log("RefreshCanonicalViews: Could not derive tenant from staging schema: " ~ staging_schema_name, warning=True) }}
        {%- endif %}
    {%- endfor -%}

    {{ log("Finished refresh_canonical_views macro.", info=True) }}

    {% if execute %}
        {{ log("Attempting explicit COMMIT at end of refresh_canonical_views", info=True) }}
        {% do run_query("COMMIT;") %}
        {{ log("Explicit COMMIT executed.", info=True) }}
    {% endif %}
{% endmacro %} 