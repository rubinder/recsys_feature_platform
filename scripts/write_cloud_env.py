"""terraform output -json  ->  .env.cloud (object names only; no secrets)."""

import json
import sys

out = {k: v["value"] for k, v in json.load(sys.stdin).items() if not v.get("sensitive")}
env = {
    "RECSYS_LAKE_BUCKET": out["lake_bucket"],
    "RECSYS_MLFLOW_BUCKET": out["mlflow_bucket"],
    "RECSYS_SF_DATABASE": out["snowflake_database"],
    "RECSYS_SF_SCHEMA": out["snowflake_schema"],
    "RECSYS_SF_WAREHOUSE": out["snowflake_warehouse"],
    "RECSYS_SF_ROLE": out["snowflake_role"],
    "RECSYS_SF_USER": out["snowflake_user"],
    "RECSYS_SF_STAGE": out["snowflake_stage"],
    "RECSYS_SF_PRIVATE_KEY_PATH": ".secrets/feast_rsa_key.p8",
}
for k, v in env.items():
    print(f"{k}={v}")
