"""Simulate the payer adjudication lifecycle on top of the Synthea extract.

Why this exists
---------------
Synthea produces claims that are charged and paid on the same day. Real
claims are not like that: they are submitted days after the service, paid
weeks later, and a share of them are adjusted, reversed or overturned on
appeal months afterwards. That lag is the whole reason "last month's cost"
keeps changing, so this script simulates it with explicit, documented
parameters (see PARAMS below). Every number produced here is synthetic.

Output (the "landing zone")
---------------------------
    landing/claims/        claim headers, delivered on the day they are submitted
    landing/claim_lines/   claim lines, delivered with the header
    landing/claim_events/  append-only financial ledger (deltas, not totals)
    landing/providers/     full provider extract on a few dates (for SCD2)
    landing/members/       full member extract
    landing/enrollment/    full enrollment extract

History is delivered as one file per month. The last DAILY_DAYS days are
delivered as one file per day, the way a production feed would arrive.

Deliberate data problems (so the pipeline has something real to handle)
-----------------------------------------------------------------------
* Redelivery: on a few dates the previous day's events are sent again.
* Schema drift: from SCHEMA_CHANGE_DATE the events feed gains a column.
* Orphans: Synthea itself leaves a handful of claims with no coverage span.

Usage:
    python generator/simulate_adjudication.py --source data/source --out data/landing
"""
from __future__ import annotations

import argparse
import shutil
import uuid
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute  # noqa: F401  (registers pa.compute)
import pyarrow.parquet as pq

SEED = 42
AS_OF_DATE = date(2026, 10, 31)      # nothing posted after this date exists yet
DAILY_DAYS = 61                      # last N days arrive as daily files
SCHEMA_CHANGE_DATE = date(2025, 7, 1)
REDELIVERY_DATES = [date(2026, 9, 9), date(2026, 9, 23), date(2026, 10, 14)]
PROVIDER_EXTRACT_DATES = [date(2023, 1, 1), date(2024, 7, 1), date(2026, 1, 1)]
EVENT_NAMESPACE = uuid.UUID("6f0e1c1e-2f6b-4d55-9f0a-5a8f0c1d2e3f")

PARAMS = {
    # days from service to submission: lognormal(median, sigma)
    "submit_lag": {"professional": (3, 0.7), "institutional": (7, 0.7), "facility_stay": (14, 0.8)},
    # days from submission to first decision
    "decision_lag": {"default": (12, 0.6), "facility_stay": (25, 0.6)},
    "denial_rate": {"GOVERNMENT": 0.05, "PRIVATE": 0.08},
    "appeal_overturn_rate": 0.18,          # share of denials later paid
    "appeal_lag_days": (30, 90),
    "allowed_ratio": {"GOVERNMENT": (0.45, 0.05), "PRIVATE": (0.65, 0.08)},  # mean, sd of allowed / billed
    "member_cost_share": {"Medicaid": 0.02, "Medicare": 0.20, "Dual Eligible": 0.03, "PRIVATE": 0.22},
    "adjustment_rate": 0.10,               # share of paid claims adjusted later
    "adjustment_lag": (45, 0.6),           # lognormal days after the payment
    "reversal_rate": 0.025,                # share of paid claims fully reversed
    "reversal_lag_days": (30, 200),
    "reprocess_after_reversal_rate": 0.60,
    "reprocess_lag_days": (5, 30),
}

DENIAL_CODES = [  # public CARC codes with plain-language meaning
    ("CO-16", "Claim lacks information needed for adjudication", 0.30),
    ("CO-50", "Not deemed a medical necessity", 0.18),
    ("CO-97", "Payment included in another service", 0.17),
    ("CO-29", "Time limit for filing has expired", 0.10),
    ("CO-18", "Exact duplicate claim or service", 0.13),
    ("PR-204", "Service not covered under the member's plan", 0.12),
]
ADJUSTMENT_REASONS = [
    ("COB", "Coordination of benefits recovery", -1),
    ("AUDIT", "Post-payment audit recoupment", -1),
    ("CONTRACT", "Contract rate correction", +1),
    ("CODING", "Coding review change", -1),
]


MONEY_COLUMNS = {"billed_amount", "charge_amount", "allowed_amount_delta", "paid_amount_delta",
                 "member_responsibility_delta"}
MONEY_TYPE = pa.decimal128(14, 2)


def _to_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write with an explicit money type.

    Letting pandas infer decimals gives each file its own precision
    (DECIMAL(7,2) in one file, DECIMAL(9,2) in the next), and the first big
    claim then fails to load. A feed should have one schema, so we pin it.
    """
    table = pa.Table.from_pandas(df, preserve_index=False)
    for i, field in enumerate(table.schema):
        if field.name in MONEY_COLUMNS:
            col = pa.compute.round(table.column(i).cast(pa.float64()), 2).cast(MONEY_TYPE)
            table = table.set_column(i, pa.field(field.name, MONEY_TYPE), col)
    pq.write_table(table, path, compression="zstd")


def _lognormal_days(rng: np.random.Generator, median: float, sigma: float, n: int, cap: int = 240) -> np.ndarray:
    return np.minimum(np.rint(rng.lognormal(np.log(median), sigma, n)).astype(int), cap)


def _event_id(claim_id: str, seq: int) -> str:
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{claim_id}:{seq}"))


def simulate(source: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(SEED)
    claims = pd.read_parquet(source / "claims.parquet").sort_values("claim_id").reset_index(drop=True)
    lines = pd.read_parquet(source / "claim_lines.parquet")
    payers = pd.read_parquet(source / "payers.parquet")

    billed = lines.groupby("claim_id")["charge_amount"].sum().astype(float).rename("billed_amount")
    claims = claims.merge(billed, on="claim_id", how="left").merge(payers, on="payer_id", how="left")
    claims["billed_amount"] = claims["billed_amount"].fillna(0.0).round(2)
    n = len(claims)

    stay = claims["encounter_class"].isin(["inpatient", "snf", "hospice"]).to_numpy()
    institutional = (claims["claim_type"] == "institutional").to_numpy()
    private = (claims["payer_type"] == "PRIVATE").to_numpy()
    service = pd.to_datetime(claims["service_date"]).to_numpy().astype("datetime64[D]")

    # --- lags -------------------------------------------------------------
    p = PARAMS
    submit_lag = np.where(
        stay, _lognormal_days(rng, *p["submit_lag"]["facility_stay"], n),
        np.where(institutional, _lognormal_days(rng, *p["submit_lag"]["institutional"], n),
                 _lognormal_days(rng, *p["submit_lag"]["professional"], n)),
    ) + 1
    decision_lag = np.where(
        stay, _lognormal_days(rng, *p["decision_lag"]["facility_stay"], n),
        _lognormal_days(rng, *p["decision_lag"]["default"], n),
    ) + 1
    submitted = service + submit_lag.astype("timedelta64[D]")
    decided = submitted + decision_lag.astype("timedelta64[D]")

    # --- money ------------------------------------------------------------
    ratio = np.where(
        private, rng.normal(*p["allowed_ratio"]["PRIVATE"], n), rng.normal(*p["allowed_ratio"]["GOVERNMENT"], n)
    ).clip(0.2, 0.95)
    allowed = (claims["billed_amount"].to_numpy() * ratio).round(2)
    share = claims["payer_name"].map(p["member_cost_share"]).fillna(p["member_cost_share"]["PRIVATE"]).to_numpy()
    member_resp = (allowed * share).round(2)
    paid = (allowed - member_resp).round(2)

    # --- outcomes ---------------------------------------------------------
    denied = rng.random(n) < np.where(private, p["denial_rate"]["PRIVATE"], p["denial_rate"]["GOVERNMENT"])
    overturned = denied & (rng.random(n) < p["appeal_overturn_rate"])
    ever_paid = ~denied | overturned
    adjusted = ever_paid & (rng.random(n) < p["adjustment_rate"])
    reversed_ = ever_paid & ~adjusted & (rng.random(n) < p["reversal_rate"])
    reprocessed = reversed_ & (rng.random(n) < p["reprocess_after_reversal_rate"])

    denial_idx = rng.choice(len(DENIAL_CODES), n, p=[d[2] for d in DENIAL_CODES])
    adj_idx = rng.integers(0, len(ADJUSTMENT_REASONS), n)
    appeal_lag = rng.integers(*p["appeal_lag_days"], n)
    adj_lag = _lognormal_days(rng, *p["adjustment_lag"], n) + 5
    adj_pct = rng.uniform(0.05, 0.35, n)
    rev_lag = rng.integers(*p["reversal_lag_days"], n)
    reproc_lag = rng.integers(*p["reprocess_lag_days"], n)
    reproc_ratio = rng.uniform(0.85, 1.05, n)
    second_of_day = rng.integers(8 * 3600, 18 * 3600, (n, 6))  # business-hours timestamps

    rows: list[tuple] = []
    cid = claims["claim_id"].to_numpy()

    def add(i: int, seq: int, etype: str, day, allowed_d=0.0, paid_d=0.0, resp_d=0.0, code=None, note=None):
        ts = pd.Timestamp(day) + pd.Timedelta(seconds=int(second_of_day[i, seq]))
        rows.append((_event_id(cid[i], seq), cid[i], etype, ts, round(allowed_d, 2), round(paid_d, 2),
                     round(resp_d, 2), code, note))

    for i in range(n):
        add(i, 0, "SUBMITTED", submitted[i])
        pay_day = decided[i]
        if denied[i]:
            code, desc, _ = DENIAL_CODES[denial_idx[i]]
            add(i, 1, "DENIED", decided[i], code=code, note=desc)
            if not overturned[i]:
                continue
            pay_day = decided[i] + np.timedelta64(int(appeal_lag[i]), "D")
            add(i, 2, "PAID", pay_day, allowed[i], paid[i], member_resp[i], code="APPEAL", note="Denial overturned on appeal")
        else:
            add(i, 1, "PAID", pay_day, allowed[i], paid[i], member_resp[i])
        if adjusted[i]:
            code, desc, sign = ADJUSTMENT_REASONS[adj_idx[i]]
            d_allowed = sign * allowed[i] * adj_pct[i]
            d_resp = d_allowed * share[i]
            add(i, 3, "ADJUSTED", pay_day + np.timedelta64(int(adj_lag[i]), "D"),
                d_allowed, d_allowed - d_resp, d_resp, code=code, note=desc)
        elif reversed_[i]:
            rev_day = pay_day + np.timedelta64(int(rev_lag[i]), "D")
            add(i, 3, "REVERSED", rev_day, -allowed[i], -paid[i], -member_resp[i], code="REVERSAL",
                note="Payment reversed in full")
            if reprocessed[i]:
                new_allowed = allowed[i] * reproc_ratio[i]
                new_resp = new_allowed * share[i]
                add(i, 4, "PAID", rev_day + np.timedelta64(int(reproc_lag[i]), "D"),
                    new_allowed, new_allowed - new_resp, new_resp, code="REPROCESS", note="Claim reprocessed after reversal")

    events = pd.DataFrame(rows, columns=[
        "event_id", "claim_id", "event_type", "event_ts", "allowed_amount_delta", "paid_amount_delta",
        "member_responsibility_delta", "reason_code", "reason_description"])
    events["posted_date"] = events["event_ts"].dt.date
    events = events[events["posted_date"] <= AS_OF_DATE].sort_values(["event_ts", "event_id"]).reset_index(drop=True)

    claims_out = claims[["claim_id", "member_id", "provider_id", "payer_id", "service_date", "encounter_class",
                         "claim_type", "primary_diagnosis_code", "encounter_reason", "billed_amount"]].copy()
    claims_out["submitted_date"] = pd.to_datetime(submitted).date
    claims_out = claims_out[claims_out["submitted_date"] <= AS_OF_DATE]
    lines_out = lines.merge(claims_out[["claim_id", "submitted_date"]], on="claim_id", how="inner")
    return claims_out, lines_out, events


def _write_partitions(df: pd.DataFrame, date_col: str, out_dir: Path, stem: str) -> int:
    """History as monthly files, the last DAILY_DAYS days as daily files."""
    out_dir.mkdir(parents=True, exist_ok=True)
    daily_start = AS_OF_DATE - timedelta(days=DAILY_DAYS - 1)
    d = pd.to_datetime(df[date_col])
    files = 0
    hist = df[d.dt.date < daily_start]
    for month, part in hist.groupby(pd.to_datetime(hist[date_col]).dt.strftime("%Y-%m")):
        _to_parquet(part, out_dir / f"{stem}_{month}.parquet")
        files += 1
    recent = df[d.dt.date >= daily_start]
    for day, part in recent.groupby(date_col):
        _to_parquet(part, out_dir / f"{stem}_{day:%Y-%m-%d}.parquet")
        files += 1
    return files


def write_landing(source: Path, out: Path) -> dict[str, int]:
    if out.exists():
        shutil.rmtree(out)
    claims, lines, events = simulate(source)
    stats = {"claims": len(claims), "claim_lines": len(lines), "claim_events": len(events)}

    # Schema drift: a new column appears partway through the feed's life.
    events["adjudication_system"] = np.where(
        pd.to_datetime(events["posted_date"]).dt.date >= date(2026, 3, 1), "NEXTGEN", "CORE")
    ev_dir = out / "claim_events"
    ev_dir.mkdir(parents=True)
    daily_start = AS_OF_DATE - timedelta(days=DAILY_DAYS - 1)
    files = 0
    for key, part in events.groupby(
        np.where(pd.to_datetime(events["posted_date"]).dt.date < daily_start,
                 pd.to_datetime(events["posted_date"]).dt.strftime("%Y-%m"),
                 pd.to_datetime(events["posted_date"]).dt.strftime("%Y-%m-%d"))):
        first_day = part["posted_date"].min()
        if first_day < SCHEMA_CHANGE_DATE:
            part = part.drop(columns=["adjudication_system"])
        # Redelivery: yesterday's rows are sent again inside today's file.
        if len(key) == 10 and date.fromisoformat(key) in REDELIVERY_DATES:
            prev = events[events["posted_date"] == date.fromisoformat(key) - timedelta(days=1)]
            part = pd.concat([part, prev], ignore_index=True)
            stats["redelivered_events"] = stats.get("redelivered_events", 0) + len(prev)
        _to_parquet(part, ev_dir / f"claim_events_{key}.parquet")
        files += 1
    stats["claim_event_files"] = files
    stats["claim_files"] = _write_partitions(claims, "submitted_date", out / "claims", "claims")
    stats["claim_line_files"] = _write_partitions(lines, "submitted_date", out / "claim_lines", "claim_lines")

    # Full extracts
    for name in ("members", "enrollment", "payers"):
        (out / name).mkdir(parents=True)
        shutil.copy(source / f"{name}.parquet", out / name / f"{name}.parquet")

    # Provider extracts on a few dates; network status drifts so SCD2 has work to do.
    rng = np.random.default_rng(SEED + 1)
    providers = pd.read_parquet(source / "providers.parquet").sort_values("provider_id").reset_index(drop=True)
    providers["network_status"] = np.where(rng.random(len(providers)) < 0.85, "in_network", "out_of_network")
    providers["network_tier"] = rng.choice(["tier_1", "tier_2"], len(providers), p=[0.6, 0.4])
    (out / "providers").mkdir(parents=True)
    changed = 0
    for k, extract_date in enumerate(PROVIDER_EXTRACT_DATES):
        if k > 0:
            flip = rng.random(len(providers)) < 0.04
            providers.loc[flip, "network_status"] = np.where(
                providers.loc[flip, "network_status"] == "in_network", "out_of_network", "in_network")
            changed += int(flip.sum())
        snap = providers.copy()
        snap["extract_date"] = extract_date
        snap.to_parquet(out / "providers" / f"providers_{extract_date:%Y-%m-%d}.parquet", index=False)
    stats["provider_network_changes"] = changed
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, default=Path("data/source"))
    ap.add_argument("--out", type=Path, default=Path("data/landing"))
    args = ap.parse_args()
    for k, v in write_landing(args.source, args.out).items():
        print(f"{k:28s} {v:>9,d}")
