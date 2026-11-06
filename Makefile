.PHONY: setup data features materialize parity train train-streaming serve latency demo test docs-capture clean \
        stream-up stream-down stream-reset backfill-streaming flink-features produce simulate metrics stream-parity demo-streaming
RUN := uv run

setup:   ## install pinned deps, check Docker and Java
	uv sync --frozen
	@docker info >/dev/null 2>&1 || (echo "Docker is not running" && exit 1)
	@java -version 2>&1 | head -1

data:    ## download + normalize MovieLens 1M into data/raw/
	$(RUN) python -m recsys.ingest.movielens

features: ## PySpark batch snapshots + streaming backfill -> data/offline/, register Feast definitions
	$(RUN) python -m recsys.features.batch
	$(RUN) python -m recsys.streaming.backfill
	$(RUN) python -m recsys.features.apply

materialize: ## start Redis and materialize features as of serving time
	docker compose up -d --wait redis
	$(RUN) python -m recsys.features.materialize

parity:  ## check Redis features equal point-in-time offline features
	$(RUN) python -m recsys.features.parity

train:   ## labels, point-in-time training set, ALS + ranker, eval, MLflow (Phase 1 baseline)
	$(RUN) python -m recsys.training.train

train-streaming: ## session-anchored rankers: batch-only vs batch + streaming features
	$(RUN) python -m recsys.training.train --feature-set batch --anchors session
	$(RUN) python -m recsys.training.train --feature-set streaming --anchors session

serve:   ## build and start Redis + API on :8000
	docker compose up -d --build --wait

latency: ## p50/p95/p99 + throughput against the running API
	$(RUN) python -m recsys.latency.loadgen

demo:    ## end to end on a fresh checkout
	$(MAKE) setup data features materialize parity train serve
	$(RUN) python scripts/demo.py

test:
	$(RUN) pytest -q

PHASE ?= 1
docs-capture: ## regenerate docs/img/ artifacts for PHASE (1: needs `make serve`; 2: after demo-streaming)
	uv run --group docs playwright install chromium
	uv run --group docs python scripts/capture_docs.py $(PHASE)

# ---- Phase 2: streaming ----------------------------------------------------
DAYS ?= 28
SPEEDUP ?= 86400
MALFORMED ?= 0

stream-up: ## Kafka, Flink (JobManager + TaskManager), Feast feature server, Redis, API
	docker compose --profile stream up -d --build --wait

stream-down:
	docker compose --profile stream down

backfill-streaming: ## streaming features over history (same logic as Flink) -> offline parts
	$(RUN) python -m recsys.streaming.backfill

stream-reset: ## topics, offline parts, Redis and logs back to the serving cutoff
	$(RUN) python -m recsys.streaming.reset

flink-features: ## (re)submit the PyFlink job
	$(RUN) python -m recsys.streaming.flink_ctl submit

produce: ## free-running replay: history bootstrap, then DAYS of events at SPEEDUP x real time
	$(RUN) python -m recsys.streaming.producer --history
	$(RUN) python -m recsys.streaming.producer --days $(DAYS) --speedup $(SPEEDUP) --malformed $(MALFORMED)

simulate: ## deterministic lockstep traffic simulation over DAYS of the test period
	$(RUN) python -m recsys.streaming.simulate --days $(DAYS)

metrics: ## CTR / hit-rate / NDCG@k / coverage from the logs (simulated outcomes)
	$(RUN) python -m recsys.eval.report

stream-parity: ## Redis + offline streaming features == shared logic recomputed from events
	$(RUN) python -m recsys.streaming.parity

demo-streaming: ## after `make demo`: train variants, live replay + DLQ, deterministic simulation, metrics
	$(MAKE) train-streaming stream-up
	docker compose restart app && docker compose up -d --wait app   # reload model variants
	$(MAKE) stream-reset flink-features
	$(RUN) python -m recsys.streaming.producer --history
	$(RUN) python -c "from recsys.streaming import progress as p, events as e; p.wait_for(p.client(), e.history_end_seq(e.ordered_events()) - 1, 1800, 0.2)"
	./scripts/demo_streaming_live.sh
	$(MAKE) stream-reset flink-features simulate metrics stream-parity

clean:
	docker compose --profile stream down -v
	rm -rf data mlruns
