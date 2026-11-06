.PHONY: setup data features materialize parity train serve latency demo test docs-capture clean
RUN := uv run

setup:   ## install pinned deps, check Docker and Java
	uv sync --frozen
	@docker info >/dev/null 2>&1 || (echo "Docker is not running" && exit 1)
	@java -version 2>&1 | head -1

data:    ## download + normalize MovieLens 1M into data/raw/
	$(RUN) python -m recsys.ingest.movielens

features: ## PySpark feature snapshots -> data/offline/, then register Feast definitions
	$(RUN) python -m recsys.features.batch
	$(RUN) python -m recsys.features.apply

materialize: ## start Redis and materialize features as of serving time
	docker compose up -d --wait redis
	$(RUN) python -m recsys.features.materialize

parity:  ## check Redis features equal point-in-time offline features
	$(RUN) python -m recsys.features.parity

train:   ## labels, point-in-time training set, ALS + ranker, eval, MLflow
	$(RUN) python -m recsys.training.train

serve:   ## build and start Redis + API on :8000
	docker compose up -d --build --wait

latency: ## p50/p95/p99 + throughput against the running API
	$(RUN) python -m recsys.latency.loadgen

demo:    ## end to end on a fresh checkout
	$(MAKE) setup data features materialize parity train serve
	$(RUN) python scripts/demo.py

test:
	$(RUN) pytest -q

docs-capture: ## regenerate docs/img/ artifacts (needs `make serve`)
	uv run --group docs playwright install chromium
	uv run --group docs python scripts/capture_docs.py

clean:
	docker compose down -v
	rm -rf data mlruns
