"""Download MovieLens 1M and normalize it into the generic schema under data/raw/."""

from __future__ import annotations

import hashlib
import io
import logging
import zipfile

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from recsys import schema
from recsys.config import RAW

URL = "https://files.grouplens.org/datasets/movielens/ml-1m.zip"
SHA256 = "a6898adb50b9ca05aa231689da44c217cb524e7ebd39d264c56e2832f2c54e20"
log = logging.getLogger(__name__)


def _read(zf: zipfile.ZipFile, name: str, cols: list[str]) -> pd.DataFrame:
    with zf.open(f"ml-1m/{name}") as f:
        text = f.read().decode("latin-1")
    rows = [line.split("::") for line in text.splitlines() if line]
    return pd.DataFrame(rows, columns=cols)


def normalize(zf: zipfile.ZipFile) -> dict[str, pa.Table]:
    r = _read(zf, "ratings.dat", ["user_id", "item_id", "rating", "ts"])
    events = pd.DataFrame(
        {
            "user_id": r.user_id.astype("int64"),
            "item_id": r.item_id.astype("int64"),
            "event_type": "rate",
            "weight": r.rating.astype("float64"),
            "timestamp": pd.to_datetime(r.ts.astype("int64"), unit="s", utc=True),
        }
    ).sort_values(["timestamp", "user_id", "item_id"], kind="stable")

    m = _read(zf, "movies.dat", ["item_id", "title", "genres"])
    items = pd.DataFrame(
        {
            "item_id": m.item_id.astype("int64"),
            "title": m.title,
            "genres": m.genres.str.split("|"),
        }
    )

    u = _read(zf, "users.dat", ["user_id", "gender", "age", "occupation", "zip"])
    attrs = [
        list(zip(["gender", "age", "occupation"], row))
        for row in u[["gender", "age", "occupation"]].itertuples(index=False)
    ]
    users_t = pa.table(
        {
            "user_id": pa.array(u.user_id.astype("int64")),
            "attributes": pa.array(attrs, type=schema.USERS.field("attributes").type),
        }
    )
    return {
        "events": schema.validate(pa.Table.from_pandas(events, preserve_index=False), schema.EVENTS),
        "items": schema.validate(pa.Table.from_pandas(items, preserve_index=False), schema.ITEMS),
        "users": schema.validate(users_t, schema.USERS),
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    RAW.mkdir(parents=True, exist_ok=True)
    cache = RAW / "ml-1m.zip"
    if not cache.exists():
        log.info("downloading %s", URL)
        resp = httpx.get(URL, timeout=120, follow_redirects=True)
        resp.raise_for_status()
        cache.write_bytes(resp.content)
    digest = hashlib.sha256(cache.read_bytes()).hexdigest()
    if SHA256 and digest != SHA256:
        raise RuntimeError(f"checksum mismatch: {digest}")
    with zipfile.ZipFile(io.BytesIO(cache.read_bytes())) as zf:
        tables = normalize(zf)
    for name, table in tables.items():
        pq.write_table(table, RAW / f"{name}.parquet")
        log.info("wrote %s: %d rows", name, table.num_rows)
    log.info("sha256 %s", digest)


if __name__ == "__main__":
    main()
