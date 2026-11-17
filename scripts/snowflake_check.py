"""Fail fast if the Snowflake login or role setup is wrong.

Runs before any load so a bad secret shows up as one clear error instead of
a confusing failure halfway through the pipeline.
"""
import base64
import hashlib
import os
import sys
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization


def public_key_fingerprint(key_path: str) -> str:
    """SHA256 fingerprint of the public half, in the format Snowflake shows
    in DESC USER (RSA_PUBLIC_KEY_FP). Safe to print: it is not a secret."""
    raw = Path(key_path).read_bytes()
    try:
        key = serialization.load_pem_private_key(raw, password=None)
    except Exception as exc:  # noqa: BLE001
        lines = raw.decode("utf-8", "replace").strip().splitlines()
        header = lines[0] if lines else "(empty)"
        looks_like_key = header.startswith("-----BEGIN") and "PRIVATE KEY" in header
        sys.exit(
            "The SNOWFLAKE_PRIVATE_KEY secret is not a readable PEM private key "
            f"({type(exc).__name__}). Lines: {len(lines)}. "
            f"First line is a key header: {looks_like_key}. "
            "Re-copy the whole .p8 file, including the BEGIN and END lines."
        )
    der = key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    return "SHA256:" + base64.b64encode(hashlib.sha256(der).digest()).decode()


print("key fingerprint:", public_key_fingerprint(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]))
print("account:", os.environ["SNOWFLAKE_ACCOUNT"], "| user:", os.environ["SNOWFLAKE_USER"])
print("connector:", snowflake.connector.__version__)

try:
    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ.get("SNOWFLAKE_ROLE", "CLAIMS_TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "CLAIMS_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "CLAIMS"),
        session_parameters={"QUERY_TAG": "claims-cost-mart:connection_check"},
    )
except Exception as exc:  # noqa: BLE001
    sys.exit(f"Snowflake login failed: {type(exc).__name__}: {exc}")

row = con.cursor().execute(
    "select current_user(), current_role(), current_warehouse(), current_database(), current_region()"
).fetchone()
print("user=%s role=%s warehouse=%s database=%s region=%s" % row)
con.close()
