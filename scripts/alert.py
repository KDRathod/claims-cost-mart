"""Send a pipeline failure alert as a GitHub issue.

Why a GitHub issue: it is free, it emails the repo owner, it is searchable
history, and closing it is the acknowledgement. One open issue per run date:
a second failure on the same date adds a comment instead of a new issue, so
retries do not spam.

Needs GITHUB_TOKEN and GITHUB_REPOSITORY (both set automatically inside
GitHub Actions). Without them it prints the alert, which is what happens on
a laptop.
"""
from __future__ import annotations

import argparse
import json
import os
import urllib.request

API = "https://api.github.com"


def _call(method: str, url: str, token: str, body: dict | None = None):
    req = urllib.request.Request(
        url, method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                 "User-Agent": "claims-cost-mart-alert"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read() or "null")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", required=True)
    ap.add_argument("--run-date", required=True)
    ap.add_argument("--message", default="")
    args = ap.parse_args()

    title = f"Pipeline failure: claims_daily {args.run_date}"
    run_url = ""
    if os.environ.get("GITHUB_RUN_ID"):
        run_url = f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    body = (
        f"**Task:** `{args.task}`\n**Run date:** {args.run_date}\n"
        f"**Target:** {os.environ.get('DBT_TARGET', 'duckdb')}\n"
        + (f"**Run log:** {run_url}\n" if run_url else "")
        + f"\n```\n{args.message.strip()[:1500]}\n```\n"
        "\nMarts were not republished after this failure. Fix, re-run the workflow, then close this issue."
    )

    token, repo = os.environ.get("GITHUB_TOKEN"), os.environ.get("GITHUB_REPOSITORY")
    if not (token and repo):
        print(f"[alert, not sent: no GITHUB_TOKEN]\n{title}\n{body}")
        return

    existing = _call("GET", f"{API}/repos/{repo}/issues?state=open&labels=pipeline-failure&per_page=50", token)
    match = next((i for i in existing if i["title"] == title), None)
    if match:
        _call("POST", f"{API}/repos/{repo}/issues/{match['number']}/comments", token, {"body": body})
        print(f"alert added to existing issue #{match['number']}")
    else:
        issue = _call("POST", f"{API}/repos/{repo}/issues", token,
                      {"title": title, "body": body, "labels": ["pipeline-failure"]})
        print(f"alert opened as issue #{issue['number']}")


if __name__ == "__main__":
    main()
