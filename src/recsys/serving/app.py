"""FastAPI app: GET /recommendations/{user_id}."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from recsys.config import load_split
from recsys.feature_repo.store import get_store
from recsys.latency.histogram import LatencyHistogram
from recsys.serving.recommender import Recommender, resolve_run_id

log = logging.getLogger("recsys.serving")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
STATE: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    hist = LatencyHistogram()
    STATE["hist"] = hist
    STATE["rec"] = Recommender(resolve_run_id(os.environ.get("MODEL_RUN_ID")), get_store(), hist)
    log.info("loaded model run %s", STATE["rec"].run_id)
    yield
    STATE.clear()


app = FastAPI(title="recsys", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "model_version": STATE["rec"].run_id}


@app.get("/recommendations/{user_id}")
def recommendations(user_id: int, n: int = Query(default=None, ge=1, le=200)) -> dict:
    rec: Recommender = STATE["rec"]
    n = n or rec.manifest["top_n"]
    try:
        strategy, items, timings = rec.recommend(user_id, n)
    except Exception as exc:  # surfaced as 503 so callers can retry
        log.exception("recommendation failed for user %s", user_id)
        raise HTTPException(status_code=503, detail="recommendation failed") from exc
    log.info("user=%s strategy=%s total_ms=%.2f stages=%s", user_id, strategy,
             timings["total"], {k: round(v, 2) for k, v in timings.items()})
    return {
        "user_id": user_id,
        "model_version": rec.run_id,
        "features_as_of": load_split().validation_end.isoformat(),
        "strategy": strategy,
        "items": [vars(i) for i in items],
        "latency_ms": {k: round(v, 3) for k, v in timings.items()},
    }


@app.get("/latency")
def latency() -> dict:
    return STATE["hist"].summary()


@app.post("/latency/reset")
def latency_reset() -> dict:
    STATE["hist"].reset()
    return {"reset": True}
