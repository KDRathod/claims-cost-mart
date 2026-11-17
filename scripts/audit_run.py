"""Post-run audit: what did this run load and publish?

Prints a short report and, inside GitHub Actions, writes it to the run
summary page. It also fails the run if the published marts do not reconcile
to the ledger, as a last line of defence that does not depend on dbt.
"""
from __future__ import annotations

import argparse
import os
import sys

QUERIES = {
    "raw_event_rows": "select count(*) from {raw}.claim_events",
    "ledger_events": "select count(*) from {marts}.fct_claim_events",
    "claims": "select count(*) from {marts}.fct_claims",
    "latest_posted_date": "select max(posted_date) from {marts}.fct_claim_events",
    "ledger_paid": "select sum(paid_amount_delta) from {marts}.fct_claim_events where not is_orphan_event",
    "claims_paid": "select sum(paid_amount) from {marts}.fct_claims",
    "pmpm_mart_paid": "select sum(paid_to_date) from {marts}.mart_cost_by_category_monthly",
    "ibnr_estimate": "select sum(ibnr_estimate) from {marts}.mart_cost_by_category_monthly where not is_mature",
    "provider_versions": "select count(*) from {marts}.dim_provider",
    "pending_claims": "select count(*) from {marts}.fct_claims where claim_status = 'pending'",
}


def connect(target: str):
    if target == "duckdb":
        import duckdb

        con = duckdb.connect(os.environ.get("DUCKDB_PATH", "data/warehouse.duckdb"), read_only=True)
        return (lambda q: con.execute(q).fetchone()[0]), {"raw": "raw", "marts": "marts"}
    import snowflake.connector

    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ.get("SNOWFLAKE_ROLE", "CLAIMS_TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "CLAIMS_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "CLAIMS"),
        session_parameters={"QUERY_TAG": "claims-cost-mart:audit"},
    )
    prefix = os.environ.get("DBT_SCHEMA_PREFIX", "")
    marts = f"{prefix}_MARTS" if prefix else "MARTS"
    return (lambda q: con.cursor().execute(q).fetchone()[0]), {"raw": "RAW", "marts": marts}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default=os.environ.get("DBT_TARGET", "duckdb"))
    ap.add_argument("--run-date", required=True)
    args = ap.parse_args()

    run, schemas = connect(args.target)
    r = {name: run(sql.format(**schemas)) for name, sql in QUERIES.items()}
    duplicates_removed = r["raw_event_rows"] - r["ledger_events"]

    lines = [
        f"### claims_daily {args.run_date} ({args.target})",
        "",
        "| Check | Value |",
        "|---|---|",
        f"| Latest posted date in ledger | {r['latest_posted_date']} |",
        f"| Raw event rows / deduplicated events | {r['raw_event_rows']:,} / {r['ledger_events']:,} |",
        f"| Redelivered rows removed | {duplicates_removed:,} |",
        f"| Claims | {r['claims']:,} ({r['pending_claims']:,} pending) |",
        f"| Paid to date (ledger) | ${float(r['ledger_paid']):,.2f} |",
        f"| IBNR estimate, immature months | ${float(r['ibnr_estimate']):,.2f} |",
        f"| Provider versions in SCD2 dimension | {r['provider_versions']:,} |",
    ]
    ok = r["ledger_paid"] == r["claims_paid"] == r["pmpm_mart_paid"]
    lines.append(f"| Ledger = claims fact = cost mart | {'yes' if ok else 'NO'} |")
    report = "\n".join(lines)
    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as fh:
            fh.write(report + "\n")
    if not ok:
        sys.exit(
            f"Reconciliation failed: ledger={r['ledger_paid']} claims={r['claims_paid']} mart={r['pmpm_mart_paid']}")


if __name__ == "__main__":
    main()
