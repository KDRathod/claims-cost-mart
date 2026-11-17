"""Replay the feed day by day and prove incremental == full rebuild.

What it does
------------
1. Builds warehouse A the way production would: load a checkpoint, run dbt,
   advance one day, load, run dbt ... (incremental models, snapshot history).
2. Builds warehouse B in one shot from all files with --full-refresh.
3. Compares the facts and marts row by row (ignoring load-time metadata).

If incremental logic drops a late event, double counts a redelivered one, or
depends on arrival order, the comparison fails.

Usage:
    python scripts/replay.py --daily-days 10     # quick (CI)
    python scripts/replay.py --daily-days 61     # the whole daily window
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2026, 10, 31)
# provider extract dates (so the snapshot sees each version) + a mid-history point
CHECKPOINTS = [date(2023, 1, 31), date(2024, 7, 31), date(2026, 1, 31)]

COMPARE = {
    "marts.fct_claim_events": "event_id, claim_id, event_type, posted_date, lag_months, paid_amount_delta, allowed_amount_delta, service_month, payer_id",
    "marts.fct_claims": "claim_id, claim_status, paid_amount, allowed_amount, member_responsibility_amount, event_count, first_paid_date, was_adjusted, was_reversed",
    "marts.mart_claims_triangle": "service_month, payer_id, service_category, lag_months, paid_in_lag, cumulative_paid",
    "marts.mart_completion_factors": "service_category, lag_months, completion_factor",
    "marts.mart_pmpm_monthly": "service_month, payer_id, member_months, paid_to_date, estimated_ultimate_paid, pmpm_paid_to_date",
}


def run(cmd: list[str], env: dict, cwd: Path = ROOT) -> None:
    res = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    if res.returncode != 0:
        print(res.stdout[-3000:], res.stderr[-2000:])
        sys.exit(f"FAILED: {' '.join(cmd)}")


def step(db: Path, through: date, env: dict, full_refresh: bool = False, lookback: int = 0) -> None:
    run([sys.executable, "ingestion/load_raw.py", "--target", "duckdb", "--through-date", through.isoformat()], env)
    cmd = ["dbt", "build", "--vars", f"{{incremental_lookback_days: {lookback}}}"]
    if full_refresh:
        cmd.append("--full-refresh")
    run(cmd, env, ROOT / "dbt")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily-days", type=int, default=10)
    args = ap.parse_args()

    a, b = ROOT / "data/replay_incremental.duckdb", ROOT / "data/replay_full.duckdb"
    for p in (a, b):
        p.unlink(missing_ok=True)
    base = {**os.environ, "DBT_TARGET": "duckdb", "DBT_PROFILES_DIR": str(ROOT / "dbt")}
    env_a = {**base, "DUCKDB_PATH": str(a)}
    env_b = {**base, "DUCKDB_PATH": str(b)}

    daily = [AS_OF - timedelta(days=i) for i in range(args.daily_days - 1, -1, -1)]
    schedule = CHECKPOINTS + [daily[0] - timedelta(days=1)] + daily
    t0 = time.time()
    for i, d in enumerate(schedule, 1):
        step(a, d, env_a)
        print(f"[incremental {i:>2}/{len(schedule)}] through {d}  ({time.time() - t0:5.1f}s)")

    step(b, AS_OF, env_b, full_refresh=True)
    print(f"[full rebuild] through {AS_OF}")

    con = duckdb.connect()
    con.execute(f"attach '{a}' as inc (read_only); attach '{b}' as fullb (read_only)")
    failed = False
    for table, cols in COMPARE.items():
        n_inc = con.execute(f"select count(*) from inc.{table}").fetchone()[0]
        n_full = con.execute(f"select count(*) from fullb.{table}").fetchone()[0]
        diff = con.execute(
            f"""select count(*) from (
                    (select {cols} from inc.{table} except select {cols} from fullb.{table})
                    union all
                    (select {cols} from fullb.{table} except select {cols} from inc.{table}))"""
        ).fetchone()[0]
        status = "OK  " if diff == 0 and n_inc == n_full else "DIFF"
        failed |= status == "DIFF"
        print(f"{status} {table:34s} incremental={n_inc:>8,d} full={n_full:>8,d} differing_rows={diff}")
    versions = con.execute(
        "select count(*), count(distinct provider_id) from inc.marts.dim_provider").fetchone()
    print(f"provider history captured by snapshot: {versions[0]:,} versions for {versions[1]:,} providers")
    if failed:
        sys.exit("incremental and full rebuild disagree")
    print("incremental build == full rebuild")


if __name__ == "__main__":
    main()
