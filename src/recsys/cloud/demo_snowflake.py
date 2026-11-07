"""`make demo-snowflake`: the same features in Snowflake, same point-in-time answers.

Requires `make terraform-apply` (writes .env.cloud) and the local pipeline
(`make demo`). Steps:
  1. upload the offline Parquet (batch views + streaming parts) to s3://<lake>/offline/<table>/
  2. as the Feast service user (key pair), create each table from the Parquet
     schema and COPY INTO it through the external stage
  3. register the cloud Feast repo (offline store = Snowflake) and run
     get_historical_features for a sample of real training rows
  4. compare with the local DuckDB result row by row
  5. materialize from Snowflake into Redis and re-run the online/offline parity check
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

import boto3
import numpy as np
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization

from recsys.cloud import env
from recsys.config import OFFLINE, RAW, load_split
from recsys.features.features import STREAMING

TABLES = ["user_stats", "user_genre_affinity", "item_popularity", "item_stats", "user_stream"]


def upload() -> None:
    s3 = boto3.client("s3")
    bucket = env("RECSYS_LAKE_BUCKET")
    for t in TABLES:
        files = sorted((OFFLINE / t).glob("*.parquet")) if (OFFLINE / t).is_dir() else [OFFLINE / f"{t}.parquet"]
        for f in files:
            s3.upload_file(str(f), bucket, f"offline/{t}/{f.name}")
        print(f"uploaded {t}: {len(files)} file(s)")


def connect():
    key = serialization.load_pem_private_key(open(env("RECSYS_SF_PRIVATE_KEY_PATH"), "rb").read(), password=None)
    der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    return snowflake.connector.connect(
        account=f"{env('SNOWFLAKE_ORGANIZATION_NAME')}-{env('SNOWFLAKE_ACCOUNT_NAME')}", user=env("RECSYS_SF_USER"),
        private_key=der, role=env("RECSYS_SF_ROLE"), warehouse=env("RECSYS_SF_WAREHOUSE"),
        database=env("RECSYS_SF_DATABASE"), schema=env("RECSYS_SF_SCHEMA"))


def load(conn) -> None:
    stage, fmt = env("RECSYS_SF_STAGE"), "PARQUET"
    cur = conn.cursor()
    for t in TABLES:
        cur.execute(f"""CREATE OR REPLACE TABLE {t.upper()} USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY ORDER_ID)
            FROM TABLE(INFER_SCHEMA(LOCATION => '@{stage}/{t}/', FILE_FORMAT => '{fmt}')))""")
        cur.execute(f"""COPY INTO {t.upper()} FROM @{stage}/{t}/ FILE_FORMAT = (FORMAT_NAME = '{fmt}')
            MATCH_BY_COLUMN_NAME = CASE_SENSITIVE""")
        n = cur.execute(f"SELECT COUNT(*) FROM {t.upper()}").fetchone()[0]
        print(f"loaded {t.upper()}: {n} rows")


def sample_entities(n: int = 2000) -> pd.DataFrame:
    """Real training-shaped rows: users and items at the four validation anchors."""
    split = load_split()
    us = pd.read_parquet(OFFLINE / "user_stats.parquet", columns=["user_id"]).user_id.unique()
    it = pd.read_parquet(OFFLINE / "item_stats.parquet", columns=["item_id"]).item_id.unique()
    rng = np.random.default_rng(split.seed)
    anchors = [split.train_end + timedelta(days=7 * i) for i in range(4)]
    return pd.DataFrame({"user_id": rng.choice(us, n), "item_id": rng.choice(it, n),
                         "event_timestamp": pd.to_datetime(rng.choice(anchors, n), utc=True)}).drop_duplicates()


def retrieve(profile: str, entities: pd.DataFrame) -> pd.DataFrame:
    """Run retrieval in a subprocess so each profile builds its own Feast store and definitions."""
    out = OFFLINE.parent / "artifacts" / f"pit_{profile}.parquet"
    entities.to_parquet(OFFLINE.parent / "artifacts" / "pit_entities.parquet")
    code = (
        "import pandas as pd; from recsys.feature_repo.store import get_store, apply; apply();"
        "from recsys.features.features import STREAMING, normalize_arrays;"
        f"e = pd.read_parquet('{OFFLINE.parent / 'artifacts' / 'pit_entities.parquet'}');"
        "df = normalize_arrays(get_store().get_historical_features(entity_df=e, features=STREAMING.refs).to_df());"
        f"df.to_parquet('{out}')"
    )
    subprocess.run([sys.executable, "-c", code], check=True, env={**os.environ, "RECSYS_PROFILE": profile})
    return pd.read_parquet(out)


def compare(local: pd.DataFrame, cloud: pd.DataFrame) -> list[str]:
    keys = ["user_id", "item_id", "event_timestamp"]
    for df in (local, cloud):
        df["event_timestamp"] = pd.to_datetime(df.event_timestamp, utc=True)
    m = local.merge(cloud, on=keys, suffixes=("_local", "_sf"))
    bad = []
    for ref in STREAMING.refs:
        c = ref.split(":")[1]
        a, b = m[f"{c}_local"], m[f"{c}_sf"]
        same = [(x is None and y is None) or (np.allclose(np.asarray(x, float), np.asarray(y, float), equal_nan=True)
                if x is not None and y is not None else False) for x, y in zip(a, b)]
        if not all(same):
            bad.append(f"{c}: {len(same) - sum(same)} rows differ")
    print(f"compared {len(m)} rows x {len(STREAMING.refs)} features (local DuckDB vs Snowflake)")
    return bad


def main() -> None:
    import sys as _sys

    if "--skip-load" not in _sys.argv:
        upload()
        with connect() as conn:
            load(conn)
    ents = sample_entities()
    bad = compare(retrieve("local", ents), retrieve("cloud", ents))
    if bad:
        print("MISMATCH:", bad)
        sys.exit(1)
    print("Snowflake point-in-time retrieval matches the local offline store")
    subprocess.run([sys.executable, "-m", "recsys.features.materialize"], check=True,
                   env={**os.environ, "RECSYS_PROFILE": "cloud"})
    subprocess.run([sys.executable, "-m", "recsys.features.parity"], check=True)
    _ = RAW  # local raw data must exist (make demo)


if __name__ == "__main__":
    main()
