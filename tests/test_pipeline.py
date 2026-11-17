"""Tests for the feed simulator and the raw loader (DuckDB, no network)."""
from __future__ import annotations

import importlib
import sys
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "generator"))
sys.path.insert(0, str(ROOT / "ingestion"))


@pytest.fixture(scope="session")
def landing(tmp_path_factory) -> Path:
    import simulate_adjudication as sim

    out = tmp_path_factory.mktemp("landing")
    sim.write_landing(ROOT / "data/source", out)
    return out


def _events(landing: Path) -> pd.DataFrame:
    return duckdb.sql(
        f"select * from read_parquet('{landing}/claim_events/*.parquet', union_by_name=true)"
    ).df()


def test_simulation_is_deterministic():
    import simulate_adjudication as sim

    a = sim.simulate(ROOT / "data/source")[2]
    b = sim.simulate(ROOT / "data/source")[2]
    pd.testing.assert_frame_equal(a, b)


def test_every_claim_starts_with_a_submission(landing):
    ev = _events(landing).drop_duplicates("event_id")
    first = ev.sort_values("event_ts").groupby("claim_id")["event_type"].first()
    assert (first == "SUBMITTED").all()


def test_reversal_cancels_the_payment_it_reverses(landing):
    ev = _events(landing).drop_duplicates("event_id").sort_values("event_ts")
    reversed_claims = ev.loc[ev.event_type == "REVERSED", "claim_id"]
    sub = ev[ev.claim_id.isin(reversed_claims)]
    # net paid up to and including the reversal is zero
    upto = sub[sub.groupby("claim_id")["event_type"].transform(lambda s: (s == "REVERSED").cumsum().shift(fill_value=0)) == 0]
    net = upto.groupby("claim_id")["paid_amount_delta"].sum().astype(float)
    assert net.abs().max() < 0.01


def test_feed_contains_redelivered_rows(landing):
    ev = _events(landing)
    assert ev.event_id.duplicated().sum() > 0, "the feed should include redeliveries for the pipeline to handle"


def test_schema_drift_column_only_in_later_files(landing):
    early = duckdb.sql(f"describe select * from '{landing}/claim_events/claim_events_2023-01.parquet'").df()
    late = duckdb.sql(f"describe select * from '{landing}/claim_events/claim_events_2026-10-31.parquet'").df()
    assert "adjudication_system" not in set(early.column_name)
    assert "adjudication_system" in set(late.column_name)


def _loader(landing: Path, db: Path, monkeypatch):
    monkeypatch.setenv("LANDING_DIR", str(landing))
    monkeypatch.setenv("DUCKDB_PATH", str(db))
    import load_raw

    return importlib.reload(load_raw)


def test_loader_is_idempotent(landing, tmp_path, monkeypatch):
    lr = _loader(landing, tmp_path / "w.duckdb", monkeypatch)
    files = lr.discover(date(2026, 9, 30))
    first = lr.load_duckdb(files)
    second = lr.load_duckdb(files)
    assert sum(first.values()) > 0
    assert second == {}, "re-running the same load must not add rows"


def test_loader_only_picks_up_new_files(landing, tmp_path, monkeypatch):
    lr = _loader(landing, tmp_path / "w.duckdb", monkeypatch)
    lr.load_duckdb(lr.discover(date(2026, 10, 1)))
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    before = con.execute("select count(*) from raw.claim_events").fetchone()[0]
    con.close()
    added = lr.load_duckdb(lr.discover(date(2026, 10, 2)))
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    after = con.execute("select count(*) from raw.claim_events").fetchone()[0]
    one_day = con.execute(
        "select count(*) from raw.claim_events where _source_file like '%2026-10-02%'").fetchone()[0]
    con.close()
    assert after - before == added["claim_events"] == one_day


def test_loader_refuses_a_file_that_changed_after_loading(landing, tmp_path, monkeypatch):
    import shutil

    mutable = tmp_path / "landing"
    shutil.copytree(landing, mutable)
    lr = _loader(mutable, tmp_path / "w.duckdb", monkeypatch)
    lr.load_duckdb(lr.discover(date(2026, 10, 1)))
    target = mutable / "claim_events" / "claim_events_2026-10-01.parquet"
    df = pd.read_parquet(target).head(1)
    df.to_parquet(target, index=False)
    with pytest.raises(RuntimeError, match="changed after it was loaded"):
        lr.load_duckdb(lr.discover(date(2026, 10, 1)))


def test_monthly_files_become_available_at_month_end():
    import load_raw

    assert load_raw.file_available_on(Path("claims_2026-02.parquet")) == date(2026, 2, 28)
    assert load_raw.file_available_on(Path("claims_2026-10-05.parquet")) == date(2026, 10, 5)
