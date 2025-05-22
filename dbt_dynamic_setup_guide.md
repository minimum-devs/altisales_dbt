
# dbt Setup Guide — Dynamic Schema Discovery  
*(Automated Staging & Canonical generation for every tenant / provider)*

> Follow these steps to let dbt create and maintain `staging` and `canonical` schemas  
> for **every** connector automatically. No `sources.yml`, no per‑tenant env‑vars.

---

## 0 • Current state

* Raw data lands in schemas named **`<slug>_raw_<provider>`**, e.g.  
  * `org_javi_2_e0eynhkr_raw_outreach`  
  * `org_pablo_s_workspace_r0trlpgs_raw_salesloft`
* The dbt project lives in `/dbt` inside the monorepo.  
* Legacy artefacts (`sources.yml`, `generate_schema_name.sql`, env‑var `DBT_TENANT_CODE`) still exist and must be removed.

---

## 1 • Delete these items

| Path / item | Why |
|-------------|-----|
| `dbt/macros/generate_schema_name.sql` | Hard‑codes `DBT_TENANT_CODE`; dynamic discovery doesn’t need it. |
| Any `sources.yml` files | We’ll discover raw schemas at run‑time. |
| Fivetran Transformations env‑var **`DBT_TENANT_CODE`** | No longer referenced. |

*(If you keep the macro, replace its content with `{{ custom_schema_name }}`.)*

---

## 2 • Leave `dbt_project.yml` untouched

```yaml
name: 'altisales_dbt'
version: '1.0.0'
profile: 'altisales_dbt'
...
```

All schema names are set via `config(schema=...)` in each model.

---

## 3 • Staging model template  

`dbt/models/staging/stage_calls.sql`

\``jinja
{%- set raw_schemas = dbt_utils.get_relations_by_prefix(schema_pattern='%\\_raw\\_%') -%}

{% for rel in raw_schemas %}
    {%- set parts     = rel.schema.split('_raw_') -%}
    {%- set tenant    = parts[0]                   -%}   {#<slug> #}
    {%- set provider  = parts[1]                   -%}

    {{ config(
         materialized = 'view',
         schema       = tenant ~ '_stg_' ~ provider,
         alias        = 'call'
    ) }}

    select
        id,
        created_at::timestamp as created_at,
        {% if provider == 'outreach' %}
            relationship_user_id               as user_id,
            relationship_call_disposition_id   as disposition_id
        {% elif provider == 'salesloft' %}
            user_id,
            disposition                        as disposition_name
        {% endif %}
    from {{ rel.schema }}.call

{% endfor %}
\```

Repeat for `user.sql`, `call_disposition.sql`, etc.

---

## 4 • Canonical model template  

`dbt/models/canonical/call.sql`

\``jinja
{%- set raw_schemas = dbt_utils.get_relations_by_prefix(schema_pattern='%\\_raw\\_%') -%}

{% for rel in raw_schemas %}
    {%- set tenant   = rel.schema.split('_raw_')[0] -%}
    {%- set provider = rel.schema.split('_raw_')[1] -%}

    {{ config(
         materialized = 'view',
         schema       = tenant ~ '_canonical',
         alias        = 'call'
    ) }}

    select * from {{ tenant }}_stg_{{ provider }}.call

{% endfor %}
\```

Add joins or aggregates as your metrics require.

---

## 5 • How updates work

1. **Connector sync** → Fivetran writes/updates `<slug>_raw_<provider>`.  
2. **Same dbt Transformation job runs**.  
   * Loop discovers new raw schema(s).  
   * Creates/refreshes `*_stg_*` views.  
   * Updates `*_canonical` views.  
3. App keeps querying `${TENANT}_canonical.call`. No code changes.

---

## 6 • Smoke‑test checklist

```bash
dbt clean && dbt deps
dbt build --target dev
psql -c "select count(*) from org_javi_2_e0eynhkr_canonical.call;"
```

Add a new connector → re‑run `dbt build` → new staging + canonical schemas appear automatically.

---

🚀  Commit, push, and watch the first run in Fivetran.
