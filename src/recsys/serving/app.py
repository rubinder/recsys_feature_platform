"""FastAPI app: GET /recommendations/{user_id}, with variants, feature cache and impression logging."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query

from recsys.feature_repo.store import get_store
from recsys.latency.histogram import LatencyHistogram
from recsys.serving.cache import TTLCache
from recsys.serving.impressions import impression_writer
from recsys.serving.recommender import Recommender, as_impression, parse_variants

log = logging.getLogger("recsys.serving")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
STATE: dict = {}


def _flag(name: str, default: str) -> bool:
    return os.environ.get(name, default).lower() in ("1", "true", "on", "yes")


@asynccontextmanager
async def lifespan(app: FastAPI):
    hist = LatencyHistogram()
    cache = TTLCache(float(os.environ.get("FEATURE_CACHE_TTL_S", "300"))) if _flag("FEATURE_CACHE", "on") else None
    variants = parse_variants(os.environ.get("MODEL_VARIANTS"))
    STATE.update(
        hist=hist,
        cache=cache,
        log_impressions=_flag("LOG_IMPRESSIONS", "on"),
        impressions=impression_writer(),
        rec=Recommender(variants, get_store(), hist, cache, os.environ.get("DEFAULT_VARIANT", "batch")),
    )
    log.info("loaded variants %s (default %s), feature cache %s", variants, STATE["rec"].default_variant,
             "on" if cache else "off")
    yield
    STATE["impressions"].flush()
    STATE.clear()


app = FastAPI(title="recsys", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    rec: Recommender = STATE["rec"]
    return {"status": "ok", "default_variant": rec.default_variant,
            "variants": {k: m.run_id for k, m in rec.models.items()}, "model_version": rec.models[rec.default_variant].run_id}


@app.get("/recommendations/{user_id}")
def recommendations(
    user_id: int,
    n: int | None = Query(default=None, ge=1, le=200),
    variant: str | None = None,
    as_of: datetime | None = Query(default=None, description="event-time 'now' (ISO 8601); default: serving cutoff"),
    cache: bool = Query(default=True, description="use the item-feature cache if enabled"),
    log_impression: bool = True,
) -> dict:
    rec: Recommender = STATE["rec"]
    if variant is not None and variant not in rec.models:
        raise HTTPException(status_code=404, detail=f"unknown variant {variant!r}")
    m = rec.models[variant or rec.default_variant]
    try:
        r = rec.recommend(user_id, n or m.manifest["top_n"], variant, as_of, cache)
    except Exception as exc:  # surfaced as 503 so callers can retry
        log.exception("recommendation failed for user %s", user_id)
        raise HTTPException(status_code=503, detail="recommendation failed") from exc
    if STATE["log_impressions"] and log_impression:
        STATE["impressions"].write(as_impression(r, user_id))
    log.info("user=%s variant=%s strategy=%s total_ms=%.2f", user_id, r.variant, r.strategy, r.timings["total"])
    return {
        "request_id": r.request_id,
        "user_id": user_id,
        "variant": r.variant,
        "model_version": r.model_version,
        "features_as_of": r.as_of.isoformat(),
        "strategy": r.strategy,
        "items": [vars(i) for i in r.items],
        "latency_ms": {k: round(v, 3) for k, v in r.timings.items()},
    }


@app.get("/latency")
def latency() -> dict:
    return STATE["hist"].summary()


@app.post("/latency/reset")
def latency_reset() -> dict:
    STATE["hist"].reset()
    if STATE["cache"] is not None:
        STATE["cache"].hits = STATE["cache"].misses = 0
    return {"reset": True}


@app.get("/cache")
def cache_stats() -> dict:
    return STATE["cache"].stats() if STATE["cache"] is not None else {"enabled": False}


@app.post("/cache/clear")
def cache_clear() -> dict:
    if STATE["cache"] is not None:
        STATE["cache"].clear()
    return {"cleared": True}


@app.post("/logs/flush")
def logs_flush() -> dict:
    w = STATE["impressions"]
    return {"flushed": w.flush(), "accepted": w.accepted, "rejected": w.rejected}
