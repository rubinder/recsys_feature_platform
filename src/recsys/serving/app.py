"""FastAPI app: GET /recommendations/{user_id}.

Variants (run ids or registry aliases), event-time as_of, item-feature cache,
experiment assignment, impression logging, Prometheus metrics, hot reload of
registry aliases (promotion / rollback without a restart).
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from prometheus_client import make_asgi_app

from recsys.experiments.assignment import active_experiment
from recsys.feature_repo.store import get_store
from recsys.latency.histogram import LatencyHistogram
from recsys.serving import metrics as M
from recsys.serving.cache import TTLCache
from recsys.serving.impressions import impression_writer
from recsys.serving.recommender import Recommender, as_impression, parse_variants

log = logging.getLogger("recsys.serving")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
STATE: dict = {}
RELOAD_EVERY_S = float(os.environ.get("REGISTRY_POLL_S", "30"))


def _flag(name: str, default: str) -> bool:
    return os.environ.get(name, default).lower() in ("1", "true", "on", "yes")


def _publish_models(rec: Recommender) -> None:
    M.MODEL_VERSION.clear()
    for name, m in rec.models.items():
        M.MODEL_VERSION.labels(name, m.run_id, m.registry_version or "").set(1)


async def _poll_registry() -> None:
    while True:
        await asyncio.sleep(RELOAD_EVERY_S)
        with suppress(Exception):
            changed = await asyncio.to_thread(STATE["rec"].reload)
            if changed:
                log.info("registry change picked up: %s", changed)
                _publish_models(STATE["rec"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    hist = LatencyHistogram()
    cache = TTLCache(float(os.environ.get("FEATURE_CACHE_TTL_S", "300"))) if _flag("FEATURE_CACHE", "on") else None
    variants = parse_variants(os.environ.get("MODEL_VARIANTS"))
    default = os.environ.get("DEFAULT_VARIANT", "champion" if "champion" in variants else "batch")
    rec = Recommender(variants, get_store(), hist, cache, default)
    STATE.update(hist=hist, cache=cache, log_impressions=_flag("LOG_IMPRESSIONS", "on"),
                 impressions=impression_writer(), rec=rec, experiment=active_experiment())
    _publish_models(rec)
    log.info("loaded variants %s (default %s), feature cache %s, experiment %s", variants, rec.default_variant,
             "on" if cache else "off", STATE["experiment"].id if STATE["experiment"] else None)
    poller = asyncio.create_task(_poll_registry())
    yield
    poller.cancel()
    STATE["impressions"].flush()
    STATE.clear()


app = FastAPI(title="recsys", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


@app.get("/health")
def health() -> dict:
    rec: Recommender = STATE["rec"]
    exp = STATE["experiment"]
    return {"status": "ok", "default_variant": rec.default_variant,
            "variants": {k: m.run_id for k, m in rec.models.items()},
            "registry_versions": {k: m.registry_version for k, m in rec.models.items() if m.registry_version},
            "model_version": rec.models[rec.default_variant].run_id, "experiment": exp.id if exp else None}


@app.get("/recommendations/{user_id}")
def recommendations(
    user_id: int,
    n: int | None = Query(default=None, ge=1, le=200),
    variant: str | None = None,
    as_of: datetime | None = Query(default=None, description="event-time 'now' (ISO 8601); default: serving cutoff"),
    cache: bool = Query(default=True, description="use the item-feature cache if enabled"),
    k: int | None = Query(default=None, ge=10, le=1000, description="candidates to score (default: model's K)"),
    log_impression: bool = True,
) -> dict:
    rec: Recommender = STATE["rec"]
    exp, arm = STATE["experiment"], None
    if variant is None and exp is not None:  # experiment assignment
        arm = exp.assign(user_id)
        if arm.model in rec.models:
            variant = arm.model
            M.ASSIGNMENTS.labels(exp.id, arm.name).inc()
        else:
            arm = None
    if variant is not None and variant not in rec.models:
        raise HTTPException(status_code=404, detail=f"unknown variant {variant!r}")
    m = rec.models[variant or rec.default_variant]
    try:
        r = rec.recommend(user_id, n or m.manifest["top_n"], variant, as_of, cache, k)
    except Exception as exc:  # surfaced as 503 so callers can retry
        M.REQUESTS.labels(variant or rec.default_variant, "error", "503").inc()
        log.exception("recommendation failed for user %s", user_id)
        raise HTTPException(status_code=503, detail="recommendation failed") from exc
    M.REQUESTS.labels(r.variant, r.strategy, "200").inc()
    M.LATENCY.labels(r.variant).observe(r.timings["total"] / 1000)
    for stage, ms in r.timings.items():
        if stage != "total":
            M.STAGE_LATENCY.labels(stage).observe(ms / 1000)
    if STATE["cache"] is not None:
        M.CACHE_HITS.set(STATE["cache"].hits)
        M.CACHE_MISSES.set(STATE["cache"].misses)
    if STATE["log_impressions"] and log_impression:
        imp = as_impression(r, user_id, experiment_id=exp.id if arm else None)
        if arm:
            imp["variant"] = arm.name  # the experiment arm; model_version records which model served it
        STATE["impressions"].write({**imp, "latency_ms": r.timings["total"]})
    return {
        "request_id": r.request_id,
        "user_id": user_id,
        "variant": arm.name if arm else r.variant,
        "model": r.variant,
        "experiment_id": exp.id if arm else None,
        "model_version": r.model_version,
        "features_as_of": r.as_of.isoformat(),
        "strategy": r.strategy,
        "items": [vars(i) for i in r.items],
        "latency_ms": {k2: round(v, 3) for k2, v in r.timings.items()},
    }


@app.post("/admin/reload")
def admin_reload() -> dict:
    changed = STATE["rec"].reload()
    _publish_models(STATE["rec"])
    return {"changed": changed, "variants": {k: m.registry_version or m.run_id[:8] for k, m in STATE["rec"].models.items()}}


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
