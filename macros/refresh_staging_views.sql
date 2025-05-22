{% macro refresh_staging_views() %}
    {{ log("Starting refresh_staging_views macro...", info=True) }}

    {% set current_database = target.database %}
    {{ log("Target database: " ~ current_database, info=True) }}

    {% set discovery_sql %}
    select distinct
        table_schema,
        table_name,
        table_type
    from {{ current_database }}.information_schema.tables
    where table_schema ilike '%_raw_%'
      and table_name ilike 'call%' {# Looking for tables starting with 'call' in raw schemas #}
    order by table_schema, table_name;
    {% endset %}
    {{ log("DEBUG: Manual discovery SQL:\n" ~ discovery_sql, info=True) }}

    {% if execute %}
        {% set discovered_relations_query_result = run_query(discovery_sql) %}
        {{ log("DEBUG: Rows returned by manual discovery SQL: " ~ discovered_relations_query_result | length, info=True) }}
    {% else %}
        {% set discovered_relations_query_result = [] %}
        {{ log("DEBUG: Skipping manual discovery SQL during parse phase.", info=True) }}
    {% endif %}

    {# Create a list of unique schema names from the discovered relations #}
    {% set unique_raw_schemas = {} %}
    {% for row in discovered_relations_query_result %}
        {% do unique_raw_schemas.update({row['table_schema']: 1}) %}
    {% endfor %}
    {{ log("DEBUG: Unique raw schemas found: " ~ (unique_raw_schemas.keys() | list), info=True) }}


    {% if unique_raw_schemas | length == 0 %}
        {{ log("No raw schemas containing tables starting with 'call' found via manual query. Macro will exit.", info=True) }}
        {{ return(None) }} 
    {% endif %}

    {# Iterate over the unique schema names to create staging views #}
    {% for raw_schema_name in unique_raw_schemas.keys() %}
        {% set schema_name_parts = raw_schema_name.split('_raw_') %}
        {% if schema_name_parts | length == 2 %}
            {% set tenant = schema_name_parts[0] %}
            {% set provider = schema_name_parts[1] %}
            {% set staging_schema = tenant ~ '_stg_' ~ provider %}

            {{ log("Processing raw schema: " ~ raw_schema_name ~ " -> staging schema: " ~ staging_schema, info=True) }}

            {# Ensure staging schema exists #}
            {% call statement('create_schema_' ~ tenant ~ '_' ~ provider, auto_begin=False) %}
                CREATE SCHEMA IF NOT EXISTS {{ staging_schema }};
            {% endcall %}
            {{ log("Ensured staging schema " ~ staging_schema ~ " exists.", info=True) }}

            {# --- CALLS VIEW --- #}
            {% set calls_sql %}
            CREATE OR REPLACE VIEW {{ staging_schema }}.call AS
            SELECT
                id AS call_id,
                created_at::timestamp AS call_created_at,
                {% if provider == 'outreach' %}
                    relationship_user_id AS user_id,
                    relationship_call_disposition_id AS call_disposition_id
                {% elif provider == 'salesloft' %}
                    user_id AS user_id,
                    disposition AS call_disposition_name
                {% else %}
                    NULL AS user_id,
                    NULL AS call_disposition_id,
                    NULL AS call_disposition_name
                {% endif %}
            FROM {{ raw_schema_name }}.call; {# Use the exact raw_schema_name and assume a 'call' table #}
            {% endset %}
            {% do run_query(calls_sql) %}
            {{ log("Created/Replaced view " ~ staging_schema ~ ".call", info=True) }}

            {# --- USERS VIEW --- #}
            {% set users_sql %}
            CREATE OR REPLACE VIEW {{ staging_schema }}.users AS
            SELECT
                id AS user_id,
                name AS user_name
            FROM {{ raw_schema_name }}.users; {# Assumes a 'users' table exists #}
            {% endset %}
            {% do run_query(users_sql) %}
            {{ log("Created/Replaced view " ~ staging_schema ~ ".users", info=True) }}

            {# --- CALL DISPOSITIONS VIEW (Outreach specific) --- #}
            {% if provider == 'outreach' %}
                {% set call_dispositions_sql %}
                CREATE OR REPLACE VIEW {{ staging_schema }}.call_disposition AS
                SELECT
                    id AS call_disposition_id,
                    name AS call_disposition_name
                FROM {{ raw_schema_name }}.call_disposition; {# Assumes a 'call_disposition' table exists #}
                {% endset %}
                {% do run_query(call_dispositions_sql) %}
                {{ log("Created/Replaced view " ~ staging_schema ~ ".call_disposition", info=True) }}
            {% endif %}

        {% else %}
            {{ log("Schema " ~ raw_schema_name ~ " does not match expected tenant_raw_provider format. Skipping.", warning=True) }}
        {% endif %}
    {% endfor %}

    {{ log("Finished refresh_staging_views macro.", info=True) }}
{% endmacro %} 