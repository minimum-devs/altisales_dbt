

Here's how we'd integrate your current dbt project to run after a Fivetran sync:

**Using Fivetran's dbt Transformations Feature:**

Fivetran's preferred method for transformations is to connect directly to your Git repository (GitHub, GitLab, etc.) where your dbt project lives.

1.  **Ensure Your dbt Project is in a Git Repository:**
    *   Your `altisales_dbt` project, including `dbt_project.yml`, `macros/refresh_staging_views.sql`, `macros/refresh_canonical_views.sql`, `models/dummy_model.sql`, and `packages.yml`, needs to be committed and pushed to a Git repository (e.g., on GitHub).

2.  **Connect Fivetran to Your Git Repository:**
    *   In your Fivetran dashboard, go to "Transformations."
    *   Add a new transformation.
    *   Choose the "dbt transformation" type.
    *   Connect Fivetran to your Git provider (GitHub, GitLab, etc.) and authorize access to the repository containing your `altisales_dbt` project.
    *   Specify the branch you want Fivetran to use (e.g., `main` or `master`).

3.  **Configure the dbt Transformation in Fivetran:**
    *   **Schedule:** You'll want to schedule these transformations to run "After a connector syncs" or "After new data arrives." You can typically choose to run it after *any* connector syncs or after *specific* connectors sync.
        *   Given your setup, running it after *any* raw source connector syncs makes sense because your `refresh_staging_views` macro will dynamically discover all `*_raw_*` schemas.
    *   **dbt Command:** You'll tell Fivetran what dbt command to execute. For your setup, this would simply be:
        `dbt run`
        *   When `dbt run` executes, it will run any models it finds (like the minimal `dummy_model.sql`). The primary purpose of this step is to get dbt to a state where it can trigger `on-run-end` hooks.
        *   Crucially, after the `run` phase completes, the `on-run-end` hooks in your `dbt_project.yml` will trigger. These will execute:
            1.  `refresh_staging_views()`: This macro dynamically discovers raw data schemas and creates/replaces your staging views (e.g., `tenant_stg_provider.call`) using DDL.
            2.  `refresh_canonical_views()`: This macro then discovers the newly created staging views and creates/replaces your canonical views (e.g., `tenant_canonical.call`) also using DDL, selecting from the staging views.
    *   **Target Name:** Fivetran will need to know which dbt target to use from your `profiles.yml`. You'll typically configure this in Fivetran's transformation setup. Fivetran often manages the `profiles.yml` content itself based on connection details you provide to it for your warehouse, or it might inject credentials. Ensure the target it uses (e.g., a target named `fivetran` or using your `prod` settings) has the necessary permissions to:
        *   Query `information_schema.tables` and `information_schema.views`.
        *   `CREATE SCHEMA IF NOT EXISTS ...`.
        *   `CREATE OR REPLACE VIEW ...` in those schemas.
        *   Read from your `*_raw_*` schemas.
    *   **dbt Version:** Ensure the dbt version Fivetran uses for its transformation environment is compatible with your project (dbt 1.9.4 in your case) and `dbt-utils`. Fivetran usually keeps up-to-date or allows you to specify versions.

4.  **Environment Variables (Important for Fivetran):**
    *   Your `profiles.yml` (the one in `~/.dbt/profiles.yml` on your local machine) is for local development. Fivetran will **not** use this file directly.
    *   When setting up the transformation or the connection to your data warehouse within Fivetran, Fivetran securely stores the credentials and effectively generates its own `profiles.yml` or connection parameters for the dbt execution environment it spins up.
    *   You typically don't need to manage `DBT_PROFILES_DIR` or provide a `profiles.yml` in your Git repo for Fivetran's dbt transformations (unless you're using very advanced custom setups). Just ensure the connection details Fivetran uses (host, user, pass, dbname) are correct and have the right permissions.

**How the Flow Works with Fivetran:**

1.  A Fivetran connector (e.g., for Outreach data which lands in `org_xxxx_raw_outreach`) syncs new data to your warehouse.
2.  After the sync completes, Fivetran triggers your configured dbt transformation.
3.  Fivetran checks out your dbt project from the specified Git branch.
4.  It runs `dbt deps` (implicitly or you might need to ensure it does) to install `dbt-utils`.
5.  It executes the command `dbt run` using the target and credentials you configured in Fivetran.
    *   Any minimal dbt models like `dummy_model.sql` run.
    *   The `on-run-end` hooks fire in sequence:
        *   `refresh_staging_views()` macro runs:
            *   It queries `information_schema.tables` in your warehouse.
            *   It discovers all `*_raw_*` schemas that have relevant tables.
            *   It creates/replaces the corresponding `*_stg_*` schemas and views (e.g., `call`, `users`, `call_disposition`).
        *   `refresh_canonical_views()` macro runs:
            *   It queries `information_schema.views` to find the staging views.
            *   It creates/replaces the corresponding `*_canonical` schemas and views, selecting from the staging views.
6.  Your staging and canonical views are now up-to-date with the latest raw data.

**Key Considerations for Fivetran:**

*   **Permissions:** The database user Fivetran uses for the dbt transformation needs all the permissions outlined above. This is the most common point of failure.
*   **Idempotency:** Your macros use `CREATE SCHEMA IF NOT EXISTS` and `CREATE OR REPLACE VIEW`, which are idempotent. This is good, as Fivetran might re-run transformations.
*   **Run Frequency:** Running this after every connector sync ensures views are always fresh. If you have many frequently syncing connectors, this means the macros run often. Given their current speed (a few seconds in your tests), this should be fine.
*   **No `DBT_TENANT_CODE` needed:** This setup beautifully avoids any need for tenant-specific environment variables, as the discovery is dynamic.

By setting this up in Fivetran, you automate the entire process from raw data landing to your staging and canonical layers being updated.
