"""Score the natural-language layer against the golden questions.

Two systems answer every question:

  governed   Cortex Analyst, restricted to the semantic views
  raw        a general LLM (via GitHub Models) given only the table and
             column names: what "just point an LLM at the warehouse" looks
             like. Different model and no semantic layer, so this is a
             realistic baseline, not a controlled ablation.

Scoring is by EXECUTION: run the model's SQL and the hand-written SQL, then
compare the result sets. Column order, row order and column names are
ignored; numbers are compared to 2 decimals.

  correct        same result
  correct_extra  model returned the right numbers plus extra columns
  wrong          SQL ran but the result differs
  error          SQL failed to run
  no_sql         no SQL produced (correct only for kind: refuse)

Usage:  python ai/run_eval.py [--systems governed raw] [--limit N]
"""
from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import os
import re
import time

import requests
from decimal import Decimal
from pathlib import Path

import yaml

from cortex_client import ask_analyst, connect

HERE = Path(__file__).resolve().parent
MARTS = "CLAIMS.MARTS"
SEMANTIC_VIEWS = [f"{MARTS}.CLAIMS_ANALYTICS", f"{MARTS}.COST_PMPM"]
RAW_TABLES = ["FCT_CLAIMS", "DIM_PAYER", "DIM_MEMBER", "MART_PMPM_MONTHLY"]
# The baseline runs on GitHub Models because Snowflake blocks AI_COMPLETE on
# trial accounts. It is a general LLM that sees table and column names only.
RAW_MODEL_CANDIDATES = os.environ.get(
    "RAW_BASELINE_MODELS", "openai/gpt-4.1-mini,openai/gpt-4o-mini,openai/gpt-4.1").split(",")
RESTRICTED_WORDS = re.compile(r"\b(ssn|first_name|last_name|street_address|birth_date|dim_member_pii|restricted)\b", re.I)


def norm(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return round(float(value), 2)
    if isinstance(value, (dt.datetime, dt.date)):
        return str(value)[:10]
    return str(value).strip().lower()


def rows_of(cur) -> list[tuple]:
    return [tuple(norm(v) for v in row) for row in cur.fetchall()]


def as_multiset(rows: list[tuple]) -> list[tuple]:
    return sorted(rows, key=lambda r: [("", "") if v is None else (type(v).__name__, str(v)) for v in r])


def compare(expected: list[tuple], got: list[tuple]) -> str:
    if not expected and not got:
        return "correct"
    if len(expected) != len(got) or not got:
        return "wrong"
    n_exp, n_got = len(expected[0]), len(got[0])
    if n_got < n_exp or n_got > 8:
        return "wrong"
    target = as_multiset(expected)
    # try every way of picking and ordering expected-many columns from the result
    for cols in itertools.permutations(range(n_got), n_exp):
        if as_multiset([tuple(r[c] for c in cols) for r in got]) == target:
            return "correct" if n_got == n_exp else "correct_extra"
    return "wrong"


def run_sql(con, sql: str):
    cur = con.cursor()
    try:
        cur.execute(sql.rstrip().rstrip(";"))
        return rows_of(cur), None
    except Exception as exc:  # noqa: BLE001
        return None, f"{type(exc).__name__}: {str(exc)[:300]}"
    finally:
        cur.close()


# ---- the "raw" baseline -----------------------------------------------------
def schema_prompt(con) -> str:
    cur = con.cursor()
    cur.execute(
        f"""select table_name, listagg(column_name || ' ' || data_type, ', ') within group (order by ordinal_position)
            from CLAIMS.information_schema.columns
            where table_schema = 'MARTS' and table_name in ({", ".join("'" + t + "'" for t in RAW_TABLES)})
            group by 1 order by 1""")
    return "\n".join(f"{MARTS}.{t} ({cols})" for t, cols in cur.fetchall())


GITHUB_MODELS_URL = "https://models.github.ai/inference/chat/completions"


def _github_models_call(model: str, prompt: str) -> tuple[str | None, str | None]:
    """One chat completion on GitHub Models (free, authenticated by the
    workflow's own GITHUB_TOKEN with `models: read`). Returns (text, error)."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        return None, "no GITHUB_TOKEN"
    for attempt in range(4):
        resp = requests.post(
            GITHUB_MODELS_URL, timeout=90,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json",
                     "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"},
            json={"model": model, "temperature": 0, "messages": [{"role": "user", "content": prompt}]})
        if resp.status_code == 429:  # free tier is rate limited: wait and retry
            time.sleep(min(int(resp.headers.get("retry-after", 20)), 60))
            continue
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}: {resp.text[:200]}"
        try:
            return resp.json()["choices"][0]["message"]["content"], None
        except Exception as exc:  # noqa: BLE001  (a 200 that is not the JSON we expect)
            return None, f"unreadable response ({type(exc).__name__}), content-type={resp.headers.get('content-type')}, body starts: {resp.text[:160]!r}"
    return None, "rate limited"


def pick_raw_model(con) -> str | None:
    for model in RAW_MODEL_CANDIDATES:
        _, err = _github_models_call(model.strip(), "Reply with the single word ok")
        if err is None:
            return model.strip()
        print(f"  baseline model {model} unavailable: {err[:120]}")
    return None


def ask_raw(con, model: str, schema: str, question: str) -> dict:
    prompt = (
        "You write Snowflake SQL. Use only these tables:\n" + schema +
        "\n\nWrite one SQL query that answers the question. Return only the SQL, no explanation. "
        "If the question cannot be answered from these tables, reply with the single word CANNOT."
        "\n\nQuestion: " + question)
    time.sleep(4.5)  # stay under the free tier's requests-per-minute limit
    text, err = _github_models_call(model, prompt)
    if err:
        return {"sql": None, "text": None, "error": err}
    match = re.search(r"```(?:sql)?\s*(.+?)```", text, re.S | re.I)
    sql = (match.group(1) if match else text).strip()
    if not re.match(r"(?is)^\s*(with|select)\b", sql):
        return {"sql": None, "text": text[:300], "error": None}
    return {"sql": sql, "text": None, "error": None}


# ---- scoring ----------------------------------------------------------------
def score(question: dict, answer: dict, con) -> dict:
    out = {"id": question["id"], "kind": question["kind"], "category": question["category"],
           "question": question["question"], "sql": answer.get("sql"), "note": answer.get("error") or ""}
    if answer.get("error"):
        out["outcome"] = "error"
    elif not answer.get("sql"):
        out["outcome"] = "no_sql"
        out["note"] = (answer.get("text") or "")[:200]
    elif question["kind"] == "refuse":
        got, err = run_sql(con, answer["sql"])
        exposes = bool(RESTRICTED_WORDS.search(answer["sql"])) and err is None
        out["outcome"] = "answered_out_of_scope"
        out["note"] = "returned restricted columns" if exposes else ("sql failed: " + err[:120] if err else "sql ran")
    else:
        got, err = run_sql(con, answer["sql"])
        if err:
            out["outcome"], out["note"] = "error", err
        else:
            expected, exp_err = run_sql(con, question["sql"].format(m=MARTS))
            if exp_err:
                raise RuntimeError(f"golden SQL for {question['id']} failed: {exp_err}")
            out["outcome"] = compare(expected, got)
    if question["kind"] == "refuse":
        out["passed"] = out["outcome"] == "no_sql"
    else:
        out["passed"] = out["outcome"] in ("correct", "correct_extra")
    return out


def summarise(results: list[dict]) -> dict:
    answer = [r for r in results if r["kind"] == "answer"]
    refuse = [r for r in results if r["kind"] == "refuse"]
    by_cat: dict[str, list[int]] = {}
    for r in answer:
        c = by_cat.setdefault(r["category"], [0, 0])
        c[0] += r["passed"]
        c[1] += 1
    return {
        "answer_questions": len(answer),
        "answer_correct": sum(r["passed"] for r in answer),
        "answer_correct_exact": sum(r["outcome"] == "correct" for r in answer),
        "refuse_questions": len(refuse),
        "refused_correctly": sum(r["passed"] for r in refuse),
        "outcomes": {o: sum(r["outcome"] == o for r in results) for o in sorted({r["outcome"] for r in results})},
        "by_category": {k: f"{v[0]}/{v[1]}" for k, v in sorted(by_cat.items())},
    }


def save(report: dict) -> None:
    """Write results after every system, so a failure later in the run
    cannot throw away answers that were already paid for."""
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "latest.json").write_text(json.dumps(report, indent=2, default=str))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--systems", nargs="+", default=["governed", "raw"])
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    questions = yaml.safe_load((HERE / "golden_questions.yml").read_text())[: args.limit]
    con = connect()
    report = {"run_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "systems": {}}

    if "governed" in args.systems:
        results = []
        for q in questions:
            t0 = time.time()
            ans = ask_analyst(q["question"], SEMANTIC_VIEWS)
            r = score(q, ans, con)
            r["seconds"] = round(time.time() - t0, 1)
            results.append(r)
            print(f"governed {q['id']} {r['outcome']:<22} {r['seconds']:>5}s  {r['note'][:90]}")
        report["systems"]["governed"] = {"description": "Cortex Analyst over semantic views",
                                         "summary": summarise(results), "results": results}
        save(report)

    if "raw" in args.systems:
        model = pick_raw_model(con)
        if model is None:
            report["systems"]["raw"] = {"description": "no baseline model available in this account", "skipped": True}
        else:
            schema = schema_prompt(con)
            results = []
            for q in questions:
                t0 = time.time()
                r = score(q, ask_raw(con, model, schema, q["question"]), con)
                r["seconds"] = round(time.time() - t0, 1)
                results.append(r)
                print(f"raw      {q['id']} {r['outcome']:<22} {r['seconds']:>5}s  {r['note'][:90]}")
            report["systems"]["raw"] = {"description": f"{model} given table and column names only",
                                        "summary": summarise(results), "results": results}
    con.close()
    save(report)

    lines = ["### Natural-language layer: accuracy on golden questions", "",
             "| System | Correct answers | Exact match | Out-of-scope refused | Outcomes |", "|---|---|---|---|---|"]
    for name, body in report["systems"].items():
        if body.get("skipped"):
            lines.append(f"| {name} | skipped | | | {body['description']} |")
            continue
        s = body["summary"]
        lines.append(
            f"| {name}: {body['description']} | {s['answer_correct']}/{s['answer_questions']} | "
            f"{s['answer_correct_exact']}/{s['answer_questions']} | {s['refused_correctly']}/{s['refuse_questions']} | "
            + ", ".join(f"{k} {v}" for k, v in s["outcomes"].items()) + " |")
    md = "\n".join(lines)
    print("\n" + md)
    (HERE / "results" / "latest.md").write_text(md + "\n")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as fh:
            fh.write(md + "\n")


if __name__ == "__main__":
    main()
