
# dbt Setup Guide — Dynamic Per‑Tenant Staging Views \(Macro Approach\)
*(Automated creation/refresh of staging & canonical artefacts for every tenant/provider)*

> Follow these steps to let **dbt** generate and maintain a per‑tenant *staging* schema  
> (e.g. `acme_stg_outreach`) for every connector. A lightweight macro runs  
> **after every `dbt run`/`dbt build`**, so no manual `run-operation` is ever required.

---

## 0 • Current state

* Raw data lands in schemas named **`<slug>_raw_<provider>`**, for example  
  * `org_javi_2_e0eynhkr_raw_outreach`  
  * `org_pablo_s_workspace_r0trlpgs_raw_salesloft`
* The dbt project lives in `/dbt` inside the monorepo.  
* Legacy artefacts (`sources.yml`, `generate_schema_name.sql`, env‑var `DBT_TENANT_CODE`) still exist and **must** be removed.

---

## 1 • Delete these items

| Path / item | Why |
|-------------|-----|
| `dbt/macros/generate_schema_name.sql` | Hard‑codes `DBT_TENANT_CODE`; dynamic discovery doesn’t need it. |
| Any `sources.yml` files | Raw schemas are discovered at run‑time. |
| Fivetran Transformations env‑var **`DBT_TENANT_CODE`** | No longer referenced. |

*(If you keep the macro, replace its body with `{{ custom_schema_name }}` so it’s inert.)*

---

## 2 • `dbt_project.yml` changes

Leave existing settings as‑is **plus** wire the refresh‑macro into the  
post‑run hook:

\\```yaml
name: "altisales_dbt"
version: "1.0.0"
profile: "altisales_dbt"

on-run-end:
  - "{{ refresh_staging_views() }}"
\\```

All schema names are still set via `config(schema=…)` **inside** each model;  
no project‑level `generate_schema_name` override is needed.

---

## 3 • Macro: create / refresh per‑tenant *staging* views

> **File:** `dbt/macros/refresh_staging_views.sql`

\\```jinja
{% macro refresh_staging_views() %}

{# Discover every `<slug>_raw_*` schema that owns a *call* table. #}
{% set raw_schemas = dbt_utils.get_relations_by_prefix(
        schema_pattern='%_raw_%',
        relation_type='table',
        table_name='call') %}

{% if raw_schemas | length == 0 %}
    {{ log("No raw schemas found — skip staging refresh", info=True) }}
    {{ return(None) }}
{% endif %}

{% for rel in raw_schemas %}
    {% set parts     = rel.schema.split('_raw_') %}
    {% set tenant    = parts[0] %}       {#<slug>#}
    {% set provider  = parts[1] %}

    {# ---- Call staging view ----------------------------------------- #}
    {% call statement('stg_call_' ~ tenant, fetch_result=False) %}
      create or replace view {{ tenant }}_stg_{{ provider }}.call as
      select  id                               as call_id,
              created_at::timestamp           as call_created_at,
              {% if provider == 'outreach' %}
                  relationship_user_id            as user_id,
                  relationship_call_disposition_id as call_disposition_id
              {% elif provider == 'salesloft' %}
                  user_id,
                  disposition                     as call_disposition_name
              {% else %}
                  null :: string                 as user_id,
                  null :: string                 as call_disposition_name
              {% endif %}
      from {{ rel.schema }}.call;
    {% endcall %}

    {# ---- Call‑disposition staging view (Outreach only) ------------- #}
    {% if provider == 'outreach' %}
      {% call statement('stg_dispo_' ~ tenant, fetch_result=False) %}
        create or replace view {{ tenant }}_stg_outreach.call_disposition as
        select id   as call_disposition_id,
               name as call_disposition_name
        from {{ rel.schema }}.call_disposition;
      {% endcall %}
    {% endif %}

{% endfor %}

{% endmacro %}
\\```

* **Idempotent**: running the macro twice merely replaces the views.  
* **Fast**: each view creation is metadata‑only; no data is copied.  
* **Extensible**: add more `statement()` blocks for other tables (`user`, `account`, …).

---

## 4 • Canonical model — one *union‑all* view for analytics

> **File:** `dbt/models/canonical/call.sql`

\\```jinja
{% set stg_calls = dbt_utils.get_relations_by_prefix(
        schema_pattern='%_stg_%', relation_type='view', table_name='call') %}

{{ config(materialized='view', schema='{{ target.schema }}_canonical', alias='call') }}

{% set selects = [] %}
{% for rel in stg_calls %}
  {% set parts     = rel.schema.split('_stg_') %}
  {% set tenant    = parts[0] %}
  {% set provider  = parts[1] %}

  {% do selects.append(
      "select call_id, call_created_at, user_id,\\n" ~
      ("call_disposition_id, cd.call_disposition_name" if provider == 'outreach' else "call_disposition_name") ~
      ", '" ~ tenant ~ "' :: string as tenant_id\\n" ~
      "from " ~ rel ~ " sc\\n" ~
      ("left join " ~ tenant ~ "_stg_outreach.call_disposition cd on sc.call_disposition_id = cd.call_disposition_id" if provider == 'outreach' else "")) %}
{% endfor %}

{% if selects %}
{{ selects | join('\\nunion all\\n') }}
{% else %}
select 1 as placeholder limit 0
{% endif %}
\\```

**Why we still union here**  
Large cross‑tenant reports (dashboards, ETLs) need the whole dataset, so a single  
union view avoids hundreds of query fragments in BI tools. Tenant‑specific queries  
meanwhile hit their own tiny `*_stg_*` views and stay fast.

*If you need isolated canonical views per tenant, copy the pattern from the macro above.*

---

## 5 • How a load cycle works

1. **Connector sync** → Fivetran writes/updates `<slug>_raw_<provider>`.  
2. **Same dbt job runs**.  
   * Macro `refresh_staging_views()` (via `on-run-end`) discovers any new *raw* schemas.  
   * Creates or replaces matching `*_stg_*` views.  
   * The canonical `call` model re‑calculates and captures the new tenant.  
3. Applications and BI keep querying  
   *Per‑tenant*: `acme_stg_outreach.call`  
   *Cross‑tenant*: `prod_canonical.call`  
   —no code changes.

---

## 6 • Smoke‑test checklist

```bash
# 1. Fresh install
 dbt clean && dbt deps

# 2. Compile & run all models in your dev target
 dbt build --target dev

# 3. Quick sanity query (replace slug)
 psql -c "select count(*) from org_javi_2_e0eynhkr_stg_outreach.call;"

# 4. Add a brand‑new connector, re‑run build
 dbt build --select tag:staging  # optional narrow run

# 5. Confirm new `*_stg_*` and canonical rows exist
 psql -c "\\dt *stg*.*call*"  # shows new views
```

Everything up‑to‑date? **Commit → push → watch the first CI/prod run.**

---

### ☑️  Next steps

* Add **schema‑level grants** if you expose per‑tenant schemas to customers.  
  Example (Snowflake): `grant usage on schema acme_stg_outreach to role ACME_RPT;`.
* Wire **tests** that count tenants vs. staging views so drift is caught automatically.
* Consider making the staging views **incremental** if raw tables are huge.
