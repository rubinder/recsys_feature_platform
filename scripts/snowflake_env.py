"""Print `export SNOWFLAKE_*=...` lines for Terraform from ~/.snowflake/connections.toml.

Usage (never echo the output):  eval "$(uv run python scripts/snowflake_env.py)"
The connection's `account` must be ORGNAME-ACCOUNTNAME. Supports token (PAT /
OAuth) and key-pair connections. Nothing is written to disk.
"""

from __future__ import annotations

import os
import shlex
import sys
import tomllib
from pathlib import Path

path = Path(os.environ.get("SNOWFLAKE_CONNECTIONS_FILE", Path.home() / ".snowflake" / "connections.toml"))
if not path.exists():
    sys.exit(f"no Snowflake connections file at {path}")
cfg = tomllib.loads(path.read_text())
conn = cfg[os.environ.get("SNOWFLAKE_CONNECTION", cfg.get("default_connection_name", "default"))]
org, _, acct = conn["account"].partition("-")
env = {
    "SNOWFLAKE_ORGANIZATION_NAME": org,
    "SNOWFLAKE_ACCOUNT_NAME": acct,
    "SNOWFLAKE_USER": conn["user"],
    "SNOWFLAKE_ROLE": conn.get("role", "ACCOUNTADMIN"),
}
if conn.get("warehouse"):
    env["SNOWFLAKE_WAREHOUSE"] = conn["warehouse"]
if conn.get("authenticator"):
    env["SNOWFLAKE_AUTHENTICATOR"] = conn["authenticator"]
if conn.get("token"):
    env["SNOWFLAKE_TOKEN"] = conn["token"]
if conn.get("private_key_file"):
    env["SNOWFLAKE_PRIVATE_KEY"] = Path(conn["private_key_file"]).expanduser().read_text()
    env.setdefault("SNOWFLAKE_AUTHENTICATOR", "SNOWFLAKE_JWT")
for k, v in env.items():
    print(f"export {k}={shlex.quote(v)}")
