"""Turn raw Synthea CSV output into compact, typed source extracts.

Synthea (https://github.com/synthetichealth/synthea) generates synthetic
patients. Nothing in this project touches real patient data.

This script keeps only what a payer-side claims warehouse would receive:
members, enrollment spans, providers, payers, claim headers and claim lines.
Real insurer names in Synthea's default config are replaced with fictional
plan names so nobody mistakes this for real payer data.

Usage:
    python generator/build_source_extract.py --synthea-csv <dir> --out data/source
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

WINDOW_START = "2023-01-01"
WINDOW_END = "2026-09-30"

# Fictional plan names. Order matches the payer rows after sorting by name.
FICTIONAL_PAYERS = {
    "Medicare": ("Medicare", "GOVERNMENT"),
    "Medicaid": ("Medicaid", "GOVERNMENT"),
    "Dual Eligible": ("Dual Eligible", "GOVERNMENT"),
    "Humana": ("Lakeshore Health Plan", "PRIVATE"),
    "Blue Cross Blue Shield": ("Prairie Mutual", "PRIVATE"),
    "UnitedHealthcare": ("Union Station Health", "PRIVATE"),
    "Aetna": ("Fox River Assurance", "PRIVATE"),
    "Cigna Health": ("Wabash Valley Health", "PRIVATE"),
    "Anthem": ("Great Plains Benefit", "PRIVATE"),
    "NO_INSURANCE": ("Self Pay", "NO_INSURANCE"),
}


def build(synthea_csv: Path, out: Path) -> dict[str, int]:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    src = synthea_csv.as_posix()

    def csv(name: str) -> str:
        return f"read_csv_auto('{src}/{name}.csv', sample_size=-1, all_varchar=true)"

    con.execute("create table payer_map(src_name varchar, plan_name varchar, ownership varchar)")
    con.executemany(
        "insert into payer_map values (?, ?, ?)",
        [(k, v[0], v[1]) for k, v in FICTIONAL_PAYERS.items()],
    )

    counts: dict[str, int] = {}

    def write(name: str, sql: str) -> None:
        path = (out / f"{name}.parquet").as_posix()
        con.execute(f"copy ({sql}) to '{path}' (format parquet, compression zstd)")
        counts[name] = con.execute(f"select count(*) from '{path}'").fetchone()[0]

    write(
        "payers",
        f"""
        select p.Id as payer_id, m.plan_name as payer_name, m.ownership as payer_type
        from {csv('payers')} p join payer_map m on p.NAME = m.src_name
        order by 1
        """,
    )

    # Claim headers inside the reporting window. The payer comes from the
    # encounter, which is what Synthea actually billed.
    con.execute(
        f"""
        create table claims as
        select
            c.Id                                   as claim_id,
            c.PATIENTID                            as member_id,
            c.PROVIDERID                           as provider_id,
            e.PAYER                                as payer_id,
            cast(c.SERVICEDATE as timestamptz)::date as service_date,
            e.ENCOUNTERCLASS                       as encounter_class,
            case c.HEALTHCARECLAIMTYPEID1 when '1' then 'professional' else 'institutional' end as claim_type,
            c.DIAGNOSIS1                           as primary_diagnosis_code,
            e.REASONDESCRIPTION                    as encounter_reason
        from {csv('claims')} c
        join {csv('encounters')} e on c.APPOINTMENTID = e.Id
        join {csv('payers')} p on e.PAYER = p.Id
        where cast(c.SERVICEDATE as timestamptz)::date between date '{WINDOW_START}' and date '{WINDOW_END}'
          and p.NAME <> 'NO_INSURANCE'
        """
    )
    write("claims", "select * from claims order by service_date, claim_id")

    write(
        "claim_lines",
        f"""
        select
            t.CLAIMID                       as claim_id,
            cast(t.CHARGEID as integer)     as line_number,
            t.PROCEDURECODE                 as procedure_code,
            t.NOTES                         as procedure_description,
            cast(t.UNITS as integer)        as units,
            cast(t.AMOUNT as decimal(12,2)) as charge_amount
        from {csv('claims_transactions')} t
        join claims c on t.CLAIMID = c.claim_id
        where t.TYPE = 'CHARGE'
        order by 1, 2
        """,
    )

    # Members: anyone with a claim or coverage in the window. Direct
    # identifiers stay in the extract on purpose: the warehouse has to mask them.
    write(
        "members",
        f"""
        select
            p.Id as member_id, p.FIRST as first_name, p.LAST as last_name,
            cast(p.BIRTHDATE as date) as birth_date, cast(p.DEATHDATE as date) as death_date,
            p.SSN as ssn, p.GENDER as gender, p.ADDRESS as street_address,
            p.CITY as city, p.STATE as state, p.ZIP as zip_code, p.COUNTY as county
        from {csv('patients')} p
        where p.Id in (select member_id from claims)
           or p.Id in (
                select PATIENT from {csv('payer_transitions')}
                where cast(END_DATE as timestamptz)::date >= date '{WINDOW_START}')
        order by 1
        """,
    )

    write(
        "enrollment",
        f"""
        select
            t.PATIENT as member_id,
            t.PAYER as payer_id,
            greatest(cast(t.START_DATE as timestamptz)::date, date '{WINDOW_START}') as coverage_start_date,
            least(cast(t.END_DATE as timestamptz)::date - 1, date '{WINDOW_END}')    as coverage_end_date,
            coalesce(t.PLAN_OWNERSHIP, 'Self') as subscriber_relationship
        from {csv('payer_transitions')} t
        join {csv('payers')} p on t.PAYER = p.Id
        where cast(t.END_DATE as timestamptz)::date > date '{WINDOW_START}'
          and cast(t.START_DATE as timestamptz)::date <= date '{WINDOW_END}'
          and p.NAME <> 'NO_INSURANCE'
        order by 1, 3
        """,
    )

    write(
        "providers",
        f"""
        select
            pr.Id as provider_id, pr.NAME as provider_name, pr.SPECIALITY as specialty,
            o.NAME as organization_name, pr.CITY as city, pr.STATE as state, pr.ZIP as zip_code
        from {csv('providers')} pr
        left join {csv('organizations')} o on pr.ORGANIZATION = o.Id
        where pr.Id in (select provider_id from claims)
        order by 1
        """,
    )
    return counts


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--synthea-csv", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=Path("data/source"))
    args = ap.parse_args()
    for name, n in build(args.synthea_csv, args.out).items():
        print(f"{name:12s} {n:>9,d} rows")
