-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) %}
    {%- set tenant = env_var('DBT_TENANT_CODE') -%}

    {%- if tenant is none -%}
        {%- do exceptions.raise_compiler_error("Environment variable DBT_TENANT_CODE is not set.") -%}
    {%- endif -%}

    {%- if custom_schema_name is none -%}
        {{- tenant }}_canonical                        {#- canonical for models without a specific schema config -#}
    {%- elif custom_schema_name.startswith('stg_') -%}
        {{- tenant }}_{{ custom_schema_name }}         {#- staging, e.g., tenant_stg_outreach -#}
    {%- else -%}
        {{- tenant }}_{{ custom_schema_name }}         {#- any other override, e.g., tenant_marts -#}
    {%- endif -%}
{% endmacro %} 