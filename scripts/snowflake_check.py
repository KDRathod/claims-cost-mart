"""Fail fast if the Snowflake login or role setup is wrong.

Runs before any load so a bad secret shows up as one clear error instead of
a confusing failure halfway through the pipeline.
"""
import os
import sys

import snowflake.connector

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
