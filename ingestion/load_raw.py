"""Idempotent raw loader: landing files -> RAW schema (DuckDB or Snowflake).

Design rules
------------
1. Append only. The loader never updates or deletes raw rows. Deduplication
   and business logic live in dbt, where they are tested and versioned.
2. A file is loaded at most once. DuckDB uses a manifest table keyed by file
   name and checksum; Snowflake uses COPY INTO's built-in load history.
   Running the loader twice in a row loads zero rows the second time.
3. Schema drift does not break the load. New columns are added to the raw
   table (DuckDB: ALTER TABLE, Snowflake: ENABLE_SCHEMA_EVOLUTION).
4. Every row carries _source_file and _loaded_at so any number can be traced
   back to the file it came from.

`--through-date` lets us replay history one day at a time, which is how the
backfill and the incremental tests simulate a daily feed.

Usage:
    python ingestion/load_raw.py --target duckdb --through-date 2026-10-31
    python ingestion/load_raw.py --target snowflake
"""
from __future__ import annotations

import argparse
import calendar
import hashlib
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

LANDING = Path(os.environ.get("LANDING_DIR", "data/landing"))
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "data/warehouse.duckdb")

# dataset -> load mode. "append" keeps every delivery; "replace" keeps the
# latest full extract only (the source system sends the whole table each time).
DATASETS = {
    "claims": "append",
    "claim_lines": "append",
    "claim_events": "append",
    "providers": "replace",
    "members": "replace",
    "enrollment": "replace",
    "payers": "replace",
}

_DATE_RE = re.compile(r"_(\d{4}-\d{2}(?:-\d{2})?)\.parquet$")


@dataclass(frozen=True)
class LandingFile:
    dataset: str
    path: Path
    available_on: date  # first day the file exists in the landing zone

    @property
    def name(self) -> str:
        return f"{self.dataset}/{self.path.name}"


def file_available_on(path: Path) -> date:
    """Daily files land on their date; monthly files land on month end."""
    m = _DATE_RE.search(path.name)
    if not m:
        return date.min  # undated full extract: always available
    token = m.group(1)
    if len(token) == 10:
        return date.fromisoformat(token)
    year, month = int(token[:4]), int(token[5:7])
    return date(year, month, calendar.monthrange(year, month)[1])


def discover(through: date) -> list[LandingFile]:
    files: list[LandingFile] = []
    for dataset in DATASETS:
        for path in sorted((LANDING / dataset).glob("*.parquet")):
            available = file_available_on(path)
            if dataset == "providers":
                # provider extracts are stamped with their extract date (first of month)
                available = date.fromisoformat(_DATE_RE.search(path.name).group(1))
            if available <= through:
                files.append(LandingFile(dataset, path, available))
    return files


def md5(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


# --------------------------------------------------------------------------
# DuckDB
# --------------------------------------------------------------------------
def load_duckdb(files: list[LandingFile]) -> dict[str, int]:
    import duckdb

    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("create schema if not exists raw")
    con.execute(
        """create table if not exists raw._load_manifest (
               file_name varchar primary key, dataset varchar, file_md5 varchar,
               row_count bigint, loaded_at timestamptz)"""
    )
    done = {r[0]: r[1] for r in con.execute("select file_name, file_md5 from raw._load_manifest").fetchall()}
    loaded: dict[str, int] = {}
    now = datetime.now(timezone.utc)

    # "replace" datasets: only the newest available extract matters.
    latest_replace: dict[str, LandingFile] = {}
    for f in files:
        if DATASETS[f.dataset] == "replace":
            cur = latest_replace.get(f.dataset)
            if cur is None or (f.available_on, f.path.name) > (cur.available_on, cur.path.name):
                latest_replace[f.dataset] = f

    for f in files:
        mode = DATASETS[f.dataset]
        if mode == "replace" and latest_replace[f.dataset] != f:
            continue
        checksum = md5(f.path)
        if done.get(f.name) == checksum:
            continue  # already loaded: this is what makes reruns safe
        if f.name in done:
            raise RuntimeError(f"{f.name} changed after it was loaded (checksum mismatch). Refusing to reload silently.")

        src = f"read_parquet('{f.path.as_posix()}')"
        select = f"select *, '{f.name}' as _source_file, timestamptz '{now.isoformat()}' as _loaded_at from {src}"
        table = f"raw.{f.dataset}"
        con.execute("begin")
        try:
            exists = con.execute(
                "select count(*) from information_schema.tables where table_schema='raw' and table_name=?",
                [f.dataset],
            ).fetchone()[0]
            if not exists or mode == "replace":
                con.execute(f"create or replace table {table} as {select}")
            else:
                # schema drift: add any column the file has that the table lacks
                have = {r[0] for r in con.execute(f"describe {table}").fetchall()}
                for col, typ, *_ in con.execute(f"describe {select}").fetchall():
                    if col not in have:
                        con.execute(f'alter table {table} add column "{col}" {typ}')
                con.execute(f"insert into {table} by name {select}")
            n = con.execute(f"select count(*) from {src}").fetchone()[0]
            con.execute(
                "insert or replace into raw._load_manifest values (?, ?, ?, ?, ?)",
                [f.name, f.dataset, checksum, n, now],
            )
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise
        loaded[f.dataset] = loaded.get(f.dataset, 0) + n
    con.close()
    return loaded


# --------------------------------------------------------------------------
# Snowflake
# --------------------------------------------------------------------------
def load_snowflake(files: list[LandingFile]) -> dict[str, int]:
    import snowflake.connector

    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],  # key-pair auth, no password
        role=os.environ.get("SNOWFLAKE_ROLE", "CLAIMS_TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "CLAIMS_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "CLAIMS"),
        schema="RAW",
        session_parameters={"QUERY_TAG": "claims-cost-mart:load_raw"},
    )
    cur = con.cursor()
    loaded: dict[str, int] = {}

    latest_replace: dict[str, LandingFile] = {}
    for f in files:
        if DATASETS[f.dataset] == "replace":
            c = latest_replace.get(f.dataset)
            if c is None or (f.available_on, f.path.name) > (c.available_on, c.path.name):
                latest_replace[f.dataset] = f

    for dataset, mode in DATASETS.items():
        todo = [f for f in files if f.dataset == dataset]
        if mode == "replace":
            todo = [latest_replace[dataset]] if dataset in latest_replace else []
        if not todo:
            continue
        for f in todo:
            # PUT skips files already staged with the same digest (OVERWRITE = FALSE)
            cur.execute(
                f"put 'file://{f.path.resolve().as_posix()}' @RAW.LANDING/{dataset}/ "
                "auto_compress = false overwrite = false parallel = 8"
            )
        table = f"RAW.{dataset.upper()}"
        pattern = "|".join(re.escape(f.path.name) for f in todo)
        cur.execute(
            f"""create table if not exists {table}
                using template (
                    select array_agg(object_construct(*))
                    from table(infer_schema(location => '@RAW.LANDING/{dataset}/', file_format => 'RAW.PARQUET_FF')))
                enable_schema_evolution = true"""
        )
        cur.execute(f"alter table {table} add column if not exists _SOURCE_FILE varchar, _LOADED_AT timestamp_tz")
        if mode == "replace":
            cur.execute(f"truncate table {table}")
        # COPY keeps 64 days of load history per table, so a file that was
        # already loaded is skipped. FORCE is only used for full-extract
        # tables that we truncated a moment ago.
        cur.execute(
            f"""copy into {table}
                from @RAW.LANDING/{dataset}/
                pattern = '.*({pattern})'
                file_format = (format_name = 'RAW.PARQUET_FF')
                match_by_column_name = case_insensitive
                include_metadata = (_SOURCE_FILE = METADATA$FILENAME, _LOADED_AT = METADATA$START_SCAN_TIME)
                {'force = true' if mode == 'replace' else ''}"""
        )
        rows = sum(r[3] for r in cur.fetchall() if len(r) > 3 and isinstance(r[3], int))
        loaded[dataset] = rows
    con.close()
    return loaded


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", choices=["duckdb", "snowflake"], default=os.environ.get("DBT_TARGET", "duckdb"))
    ap.add_argument("--through-date", type=date.fromisoformat, default=date.max,
                    help="only load files that existed on or before this date")
    args = ap.parse_args()

    files = discover(args.through_date)
    loaded = load_duckdb(files) if args.target == "duckdb" else load_snowflake(files)
    total = sum(loaded.values())
    print(f"files visible: {len(files)} | rows loaded this run: {total:,}")
    for k, v in sorted(loaded.items()):
        print(f"  {k:14s} {v:>9,d}")


if __name__ == "__main__":
    main()
