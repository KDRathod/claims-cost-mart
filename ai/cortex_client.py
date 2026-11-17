"""Small client for Snowflake: SQL over the connector, Cortex Analyst over REST.

Both use the same key pair as the pipeline. The REST call signs a short-lived
JWT with the private key (no token is stored anywhere).
"""
from __future__ import annotations

import base64
import hashlib
import os
import time
from pathlib import Path

import jwt
import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization

ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
USER = os.environ["SNOWFLAKE_USER"]
KEY_PATH = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
HOST = f"https://{ACCOUNT.lower()}.snowflakecomputing.com"


def connect():
    return snowflake.connector.connect(
        account=ACCOUNT, user=USER, private_key_file=KEY_PATH,
        role=os.environ.get("SNOWFLAKE_ROLE", "CLAIMS_TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "CLAIMS_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "CLAIMS"),
        schema="MARTS",
        session_parameters={"QUERY_TAG": "claims-cost-mart:ai_eval", "STATEMENT_TIMEOUT_IN_SECONDS": 120},
    )


def _jwt() -> str:
    key = serialization.load_pem_private_key(Path(KEY_PATH).read_bytes(), password=None)
    der = key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    fingerprint = "SHA256:" + base64.b64encode(hashlib.sha256(der).digest()).decode()
    qualified_user = f"{ACCOUNT.upper()}.{USER.upper()}"
    now = int(time.time())
    return jwt.encode(
        {"iss": f"{qualified_user}.{fingerprint}", "sub": qualified_user, "iat": now, "exp": now + 3000},
        key, algorithm="RS256")


def ask_analyst(question: str, semantic_views: list[str], timeout: int = 120) -> dict:
    """Send one question to Cortex Analyst. Returns {sql, text, raw, error}."""
    body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
        "semantic_models": [{"semantic_view": v} for v in semantic_views],
    }
    resp = requests.post(
        f"{HOST}/api/v2/cortex/analyst/message", json=body, timeout=timeout,
        headers={"Authorization": f"Bearer {_jwt()}", "Content-Type": "application/json",
                 "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"})
    if resp.status_code != 200:
        return {"sql": None, "text": None, "raw": None, "error": f"HTTP {resp.status_code}: {resp.text[:400]}"}
    data = resp.json()
    content = data.get("message", {}).get("content", [])
    sql = next((c.get("statement") for c in content if c.get("type") == "sql"), None)
    text = " ".join(c.get("text", "") for c in content if c.get("type") == "text") or None
    return {"sql": sql, "text": text, "raw": data, "error": None}
