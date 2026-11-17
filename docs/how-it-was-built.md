# How this was built, step by step

Each step says what was done, why it matters in production, and why this tool
was chosen over the alternatives. Errors hit along the way are in
[build-log.md](build-log.md).

## Step 0. Start from the business question, not the tools

**Question:** "Why did last month's medical cost change after we reported it?"

A health plan reports cost per member per month (PMPM). The number for a
recent month keeps moving for months, because claims are submitted days after
the care, paid weeks later, and some are adjusted, reversed or overturned on
appeal long after that. Finance needs two things: a number that reconciles to
the cent, and an honest estimate of where an unfinished month will land.

Everything in the repo follows from that: an append-only ledger (so history is
never lost), a run-out triangle (so we know what was known when), and
completion factors (so recent months can be estimated).

## Step 1. Get data that behaves like the real thing

- **What:** generated 3,321 synthetic patients with [Synthea](https://github.com/synthetichealth/synthea), kept the payer-side tables (members, enrollment, providers, claims, claim lines), then wrote `generator/simulate_adjudication.py` to add the adjudication lifecycle.
- **Why:** profiling showed Synthea pays every claim on the day of service, with no adjustments at all. That hides the exact problem this project is about. The simulator adds submission lag, decision lag, denials (5 to 8%), appeals, adjustments (10% of paid claims) and reversals (2.5%), each with a documented parameter.
- **Why it matters:** a pipeline is only as tested as its data is nasty. The feed also redelivers rows on three dates and gains a column in July 2025, because real feeds do both.
- **Honest limit:** the lag behaviour is simulated and stable over time. Real lag shifts with staffing, system migrations and seasonality.

## Step 2. Land files, then load raw tables idempotently

- **What:** `ingestion/load_raw.py` loads parquet files into a RAW schema. History arrives as monthly files, the last weeks as daily files.
- **Rules it follows:** append only; a file is loaded at most once; schema drift adds a column instead of failing; every row carries `_source_file` and `_loaded_at`.
- **How idempotency works:** on DuckDB, a manifest table keyed by file name and checksum. On Snowflake, `COPY INTO` keeps its own load history, so a file already loaded is skipped. A file whose content changed after loading is refused loudly rather than reloaded silently.
- **Why it matters:** retries are a fact of life (a runner dies, a task is cleared). If a retry can double load, every downstream number is suspect.
- **Why not Fivetran or Airbyte:** the source here is files, not an API or database, and the loader is 250 lines. A managed connector adds cost and a black box for no gain. For a SaaS source I would use one.

## Step 3. Model it in dbt: staging, facts, marts

- **Staging** renames and types, one model per source table, no business logic.
- **`fct_claim_events`** is the deduplicated ledger. It is incremental on `_loaded_at` (when we received a row), not on the business date. A file that arrives three weeks late has an old business date but a fresh `_loaded_at`, so it is still picked up. Filtering on the business date is the classic way late data gets dropped.
- **`fct_claims`** holds each claim's current state. On each run it rebuilds only the claims that got a new event, always from their full ledger, and merges on `claim_id`. Rebuilding from the full ledger makes the result independent of arrival order.
- **`dim_provider`** is a Type 2 dimension from a dbt snapshot. Claims join to it point-in-time (`service_date` between `valid_from` and `valid_to`), so a claim is reported under the network status the provider had on the day of service.
- **Why dbt:** tests, docs, lineage and incremental logic in version-controlled SQL. Stored procedures give none of that.
- **Why deltas in the ledger:** each event stores the change, not the new total. Any total is then a plain sum, and a reversal is just a negative row. No updates, no deletes.

## Step 4. Answer the business question: triangle, completion factors, IBNR

- **`mart_claims_triangle`** stores, for each service month, cumulative paid after 0, 1, 2 ... months. It answers "what did we know, and when?".
- **`mart_completion_factors`** is the share of final cost usually paid after N months, taken only from mature months (12 or more months of run-out) and kept per service category, because a hospital stay settles far more slowly than an office visit.
- **`mart_pmpm_monthly`** publishes paid-to-date PMPM next to a completion-adjusted estimate, with IBNR (incurred but not reported) as the gap.
- **`mart_ibnr_backtest`** checks the estimate honestly: for each mature month it rebuilds the estimate that would have been made at the time, using only factors known then, and compares it with the real final cost.

Backtest result on this data (mean absolute error against final cost):

| Months of run-out | Paid to date | Estimate |
|---|---|---|
| 0 | 74.3% | 12.5% |
| 1 | 10.6% | 4.0% |
| 2 | 1.3% | 1.8% |
| 3+ | 1.0% or less | same |

So the estimate earns its place for the first two reporting cycles and should be dropped after that.

## Step 5. Prove it, do not assume it

- **72 data tests:** keys, relationships, accepted values, ranges.
- **Reconciliation tests:** every dollar in the ledger appears exactly once in the claims fact, the cost mart and the triangle. This catches fan-out joins and dropped rows.
- **A unit test** pins claim status logic (reversed, denied then paid on appeal, pending, adjusted).
- **`scripts/replay.py`** loads the feed day by day through the incremental path, builds a second warehouse in one shot with `--full-refresh`, and diffs five facts and marts. Zero differing rows means the incremental logic is equivalent to a full rebuild.
- **Why it matters:** the replay found a bug no single build could: staging referenced a column that did not exist yet in early history.

## Step 6. Move it to Snowflake with least privilege

- **What:** `snowflake/01_setup.sql` creates one X-Small warehouse (60-second auto-suspend) under a resource monitor, four roles, masking and row access policies, and a service user that logs in with a key pair.
- **Why key pair:** there is no password to leak, rotate or paste into a config file. The private key lives only in GitHub Actions secrets.
- **PII design:** direct identifiers exist in exactly one model, in a RESTRICTED schema, with masking policies on top. The marts carry only de-identified fields (birth year, age band, 3-digit ZIP).
- **Why Snowflake:** it is the warehouse most analytics engineering roles ask for, and its governance features (masking, row access) are first class. BigQuery would have been the familiar choice; this was a deliberate step outside it.
- **Why a DuckDB target too:** the same dbt project runs locally and in CI for free, and keeps working after the Snowflake trial ends.

## Step 7. Orchestrate with Airflow

- **DAG:** `land_feed -> check_warehouse -> load_raw -> source_freshness -> dbt_build -> audit_run`.
- **Principles:** the DAG only orchestrates; every task is a thin call that also runs by hand. Tasks are keyed on the logical date, so any run can be repeated. `max_active_runs=1` stops two loads interleaving. Network-facing tasks retry with backoff; a failed data test does not retry, it alerts.
- **Alerting:** a failed task opens one GitHub issue per run date (a second failure adds a comment). Free, emailed, searchable.
- **Where it runs:** there is no always-on server on a free tier, so GitHub Actions starts the DAG on a schedule with `airflow dags test`. On MWAA, Astronomer or Composer the same file runs unchanged.
- **Why Airflow over Dagster or Prefect:** it is still what most job descriptions name. Dagster's asset model is arguably a better fit for dbt-centric work, and I say so if asked.

## Step 8. CI

Every push runs, on DuckDB with no secrets: unit tests, a full dbt build with all data tests, a 5-day replay, and the whole Airflow DAG. A pull request that breaks incremental logic, a test or the DAG fails before merge.

## Step 9. The AI layer, with a score

- **What:** two Snowflake semantic views define the metrics once (denial rate, PMPM, IBNR) over de-identified marts. Cortex Analyst turns questions into SQL using only those views.
- **Why a semantic layer:** an LLM pointed at raw tables has to guess joins and metric definitions. PMPM is the trap: averaging row-level PMPM is wrong, total paid over total member months is right. The semantic view encodes that once.
- **How it is measured:** 26 answerable questions with hand-written SQL, plus 4 that should be refused (PII, forecasts, data that does not exist). The eval runs the model's SQL and compares result sets, not SQL text. A raw text-to-SQL baseline answers the same questions with table and column names only.
- **Results:** see the README. The numbers come from `ai/results/latest.json`, written by the eval run.

## Tool choices in one table

| Need | Chosen | Main alternative | Why |
|---|---|---|---|
| Warehouse | Snowflake | BigQuery | Market demand, masking and row policies |
| Transform | dbt Core | Stored procedures, SQLMesh | Tests, docs, lineage, incremental models |
| Orchestrate | Airflow | Dagster, Prefect | Most requested; DAG is thin by design |
| Local and CI engine | DuckDB | Postgres | Zero setup, same SQL shape, free CI |
| History | dbt snapshot | Hand-written SCD2 merge | Less code to get wrong |
| Monitoring | dbt tests, freshness, volume check, audit | Elementary, Monte Carlo | Enough at this size, nothing to host |
| Alerting | GitHub issues | Slack, PagerDuty | Free and durable; swap the script for Slack in a team |
| NL to SQL | Cortex Analyst + semantic views | Raw LLM on tables | Governed metrics and no PII exposure |
