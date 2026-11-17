# Build log: errors hit and how they were fixed

Kept as the project was built. Each entry: what broke, why, the fix, and the
production lesson.

## 1. `Casting value "199440.00" to type DECIMAL(7,2) failed`
- **What happened:** the loader created the raw table from the first file it saw. That file's largest amount fit in DECIMAL(7,2), so that became the column type. A later file held a $199,440 hospital claim and the insert failed.
- **Why:** pandas picked a different decimal precision for every parquet file, so the "schema" of the feed depended on the data in each file.
- **Fix:** the generator now writes money columns with one pinned type, DECIMAL(14,2), in every file.
- **Lesson:** never let a table's types be inferred from a sample. A feed needs one declared schema, and the first unusually large value is what finds the gap.

## 2. `dbt expects 1 package(s) ... found only 0` after `dbt deps` failed
- **What happened:** `dbt deps` could not reach hub.getdbt.com from the build sandbox (blocked by a network allowlist).
- **Fix:** install dbt_utils straight from its GitHub repo, pinned to a release tag.
- **Lesson:** pin package versions, and know that the dbt Hub is just an index over git repos. Locked-down CI runners often need the git form.

## 3. `Table with name snap_providers does not exist`
- **What happened:** ran `dbt snapshot` and then `dbt run`. The snapshot reads `stg_providers`, a view that did not exist yet, so the snapshot failed and `dim_provider` failed after it.
- **Fix:** use `dbt build`, which runs models, snapshots and tests together in dependency order.
- **Lesson:** `snapshot` then `run` is the old habit. `build` is safer because it respects the DAG and stops downstream models when a test fails.

## 4. Unit test: `Set operations can only apply to expressions with the same number of result columns`
- **What happened:** expected rows in a dbt unit test listed different columns per row.
- **Why:** dbt turns each fixture row into a SELECT and UNIONs them, so every row needs the same columns.
- **Fix:** list the same columns in every expected row.

## 5. `Column "adjudication_system" referenced ... cannot be referenced before it is defined`
- **What happened:** found by the day-by-day replay. The staging model selected `adjudication_system`, a column the feed only gained in July 2025. Replaying from 2023, the raw table did not have the column yet and staging failed.
- **Fix:** `column_or_null()` macro checks the real table at compile time and falls back to a typed NULL.
- **Lesson:** schema drift cuts both ways. Handling a new column in the loader is not enough if the transform assumes the column always existed. The one-shot build never showed this; only replaying history did.

## 6. `250001: Failed to login: Authentication failed. UnexpectedError`
- **What happened:** the first Snowflake run in GitHub Actions failed at login with a vague error. Snowflake's own login history showed no attempt for the service user at all.
- **Why:** the `SNOWFLAKE_PRIVATE_KEY` secret did not contain the key. It was a single line, not the multi-line PEM file, so the driver had nothing valid to sign with and never got as far as naming a user.
- **How it was found:** secrets cannot be read back, so the login check now parses the key file first and prints two safe facts: how many lines it has and whether the first line is a key header. It also prints the public-key fingerprint, which can be compared to `RSA_PUBLIC_KEY_FP` in `DESC USER`.
- **Fix:** paste the whole `.p8` file into the secret again, BEGIN and END lines included.
- **Lesson:** fail fast and say why. A pipeline should check its own credentials before doing work, and diagnose them without ever printing the secret.

## 7. `column '_LOADED_AT' already exists` on the second Snowflake load
- **What happened:** the first load worked, the second failed on `ALTER TABLE ... ADD COLUMN IF NOT EXISTS _SOURCE_FILE varchar, _LOADED_AT timestamp_tz`.
- **Why:** in a multi-column `ADD COLUMN`, Snowflake applies `IF NOT EXISTS` to the first column only. On the second run the first column was skipped and the second raised an error.
- **Fix:** one `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statement per column.
- **Lesson:** "runs once" is not "idempotent". The bug only appears on the second run, which is why the loader is always tested by running it twice.

## 8. Semantic view: `invalid identifier 'PROVIDER_NETWORK_STATUS'`
- **What happened:** `CREATE SEMANTIC VIEW` failed on one dimension.
- **Why:** the syntax is `table.new_name AS expression`, the reverse of a normal SQL alias (`expression AS name`). I wrote it the SQL way round, so Snowflake looked for a column named after my alias.
- **Fix:** `claims.provider_network_status AS provider_network_status_at_service`.

## 9. Semantic view: `Cyclic reference of expressions is not allowed`
- **What happened:** a metric called `estimated_ultimate_paid` was built on a fact that read the column `estimated_ultimate_paid`.
- **Why:** inside a semantic view, a name resolves to the semantic object first. The fact's expression pointed at the metric, which pointed back at the fact.
- **Fix:** metric names differ from the physical column names (`total_estimated_ultimate_paid`).

## 10. `SNOWFLAKE.CORTEX.COMPLETE is not available on this account` then `AI function ... is not available for trial accounts`
- **What happened:** the raw text-to-SQL baseline could not call an LLM inside Snowflake.
- **Why:** two separate limits. New accounts are blocked from the legacy `SNOWFLAKE.CORTEX.*` functions (use `AI_COMPLETE`), and trial accounts cannot call `AI_COMPLETE` at all. Cortex Analyst itself is allowed on a trial.
- **Fix:** the baseline now calls GitHub Models from the workflow, authenticated by the workflow's own token (`permissions: models: read`), so no extra account or key is needed.
- **Lesson:** check what a free tier actually permits before designing around it, and keep the model behind one small function so the provider can be swapped.

## 11. Eval crashed after the expensive half and lost its results
- **What happened:** all 30 Cortex Analyst questions ran, then the baseline's first call returned something that was not JSON, the script raised, and nothing had been written to disk. The Analyst answers were paid for and gone.
- **Fix:** results are saved after each system finishes, the baseline call reports an unreadable response as an error instead of raising, and the workflow can run one system on its own (`systems: raw`) so a cheap part can be debugged without re-running the expensive part.
- **Lesson:** checkpoint after every costly step. This is the same idea as an idempotent loader: a failure late in a run should not force you to redo, or re-pay for, the work before it.
