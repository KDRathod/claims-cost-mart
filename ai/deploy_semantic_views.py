"""Create or replace the semantic views from snowflake/02_semantic_views.sql."""
from pathlib import Path

from cortex_client import connect

sql = Path(__file__).resolve().parents[1].joinpath("snowflake/02_semantic_views.sql").read_text()
con = connect()
for cur in con.execute_string(sql):
    row = cur.fetchone()
    print(row[0] if row else "ok")
con.close()
