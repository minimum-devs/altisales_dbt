**Cursor Rules & Learnings from This Process:**

1.  **Verify Tool Output Independently When Possible:**
    *   **Learning:** The `dbt_utils.get_relations_by_prefix` macro was a black box initially. We assumed its output, but it wasn't behaving as expected.
    *   **Rule:** When a utility or external macro (like those from `dbt-utils`) doesn't produce the expected results, try to replicate its core logic with simpler, direct operations if possible (e.g., running the underlying SQL query manually via `psql` or a `run_query` within dbt). This helps isolate whether the issue is with the tool itself, its usage, or the underlying system.

2.  **Trust, but Verify, File Edits:**
    *   **Learning:** We had instances where `edit_file` diffs looked correct, but subsequent dbt runs indicated the problematic characters (like extraneous backslashes) were still present.
    *   **Rule:** If an `edit_file` operation is intended to fix a very specific character-level issue (especially around quotes, backslashes, or special characters in Jinja/SQL) and the problem persists, explicitly use `read_file` immediately after the edit to confirm the *actual* content on disk before re-running the primary task (e.g., `dbt run`). If a discrepancy is found, consider a more robust edit or manual intervention.

3.  **Iterate on Arguments for External Macros/Functions Systematically:**
    *   **Learning:** We cycled through positional vs. keyword arguments for `get_relations_by_prefix` and the `database` argument.
    *   **Rule:** When dealing with an external function/macro with unclear or version-dependent argument handling:
        *   Start with the documented signature for the *specific version* being used.
        *   If errors occur (e.g., "takes no keyword argument" or "missing positional argument"), methodically test variations: all positional, all keyword (if supported), common keyword overrides.
        *   Prioritize official documentation for the exact version of the library/package.

4.  **Log Extensively During Macro Development and Debugging:**
    *   **Learning:** Adding `log()` statements at each step of the `refresh_staging_views` macro was crucial for understanding its flow, variable states (like `target.database`), and the results of intermediate operations (like the row count from our manual SQL query).
    *   **Rule:** When developing or debugging complex dbt macros (especially those with loops, conditionals, or external calls):
        *   Use `{{ log("message: " ~ variable, info=True) }}` liberally to inspect variable values.
        *   Log entry and exit points of key logic blocks.
        *   Log the results of conditions (`if x then log("x is true")`).
        *   Log counts of items in loops or results from queries.

5.  **Understand dbt's Parse vs. Execute Phases (`{% if execute %}`):**
    *   **Learning:** Our initial attempt to debug `get_relations_by_prefix` by calling `run_query()` inside the macro didn't yield results immediately because it was "Skipping run_query during parse phase."
    *   **Rule:** Be mindful of when `run_query()` and other database-interacting operations will actually execute. Operations within `on-run-start`/`on-run-end` hooks, or in models, generally run during the execution phase. However, logic *within* macros might be evaluated during parsing too. Use `{% if execute %}` blocks to ensure database queries run only when intended and possible.

6.  **Isolate the Problem: Staging First, Then Canonical:**
    *   **Learning:** Your decision to focus on getting the staging layer working before tackling the canonical layer was a good strategy. It simplified the debugging scope.
    *   **Rule:** When implementing a multi-layered data transformation, test and validate each layer independently before building subsequent layers on top. This makes it easier to pinpoint where issues are arising.

7.  **When a Utility Fails Persistently, Consider Manual Implementation:**
    *   **Learning:** `dbt_utils.get_relations_by_prefix` simply wasn't working for us despite extensive troubleshooting. Re-implementing its core purpose (listing specific tables in specific schemas) with a direct `information_schema` query via `run_query()` was the ultimate fix.
    *   **Rule:** If a utility function/macro from an external package consistently fails to work in your specific environment after reasonable debugging, and you understand its underlying logic, consider implementing that core logic directly within your project's macros. This gives you full control and transparency, though it means maintaining that logic yourself.

8.  **The `dbt run` "Nothing to do" and Hooks:**
    *   **Learning:** An empty `dbt run` (no models selected/found) led to the `on-run-end` hook's logs not appearing initially. Adding a dummy model forced a "run" and made the hook's execution visible.
    *   **Rule:** When testing `on-run-start` or `on-run-end` hooks, ensure dbt performs a complete run cycle. If no models are enabled, add a temporary dummy model to ensure the main run phase executes, which then reliably triggers the hooks.
