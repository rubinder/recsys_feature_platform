.PHONY: setup data features materialize parity train train-streaming serve latency demo test docs-capture clean \
        pipeline dagster-dev quality freshness \
        snowflake-keys terraform-bootstrap terraform-init terraform-plan terraform-apply terraform-destroy \
        terraform-outputs demo-snowflake benchmarks benchmarks-report \
        stream-up stream-down flink-ui stream-reset backfill-streaming flink-features produce simulate metrics stream-parity demo-streaming
RUN := uv run
DAGSTER_HOME := $(CURDIR)/data/dagster
DAGSTER := mkdir -p $(DAGSTER_HOME) && touch $(DAGSTER_HOME)/dagster.yaml && DAGSTER_HOME=$(DAGSTER_HOME) uv run dagster
DEFS := -m recsys.orchestration.definitions

setup:   ## install pinned deps, check Docker and Java
	uv sync --frozen
	@docker info >/dev/null 2>&1 || (echo "Docker is not running" && exit 1)
	@java -version 2>&1 | head -1

# Batch targets are thin wrappers over Dagster assets (each runs its quality checks).
pipeline: ## the whole batch path as one Dagster job: ingest -> features -> Feast -> rankers -> metrics
	docker compose up -d --wait redis
	$(DAGSTER) job execute $(DEFS) -j batch_pipeline

data:    ## asset raw_events: download + normalize MovieLens 1M into data/raw/
	$(DAGSTER) asset materialize $(DEFS) --select raw_events

features: ## assets batch_features, streaming_backfill, feast_registry
	$(DAGSTER) asset materialize $(DEFS) --select batch_features,streaming_backfill,feast_registry

materialize: ## asset online_store (starts Redis): features as of serving time
	docker compose up -d --wait redis
	$(DAGSTER) asset materialize $(DEFS) --select online_store

parity:  ## check Redis features equal point-in-time offline features
	$(RUN) python -m recsys.features.parity

train:   ## asset ranker_batch: Phase 1 baseline (weekly anchors, batch features)
	$(DAGSTER) asset materialize $(DEFS) --select ranker_batch

train-streaming: ## assets ranker_batch_session, ranker_streaming_session
	$(DAGSTER) asset materialize $(DEFS) --select ranker_batch_session,ranker_streaming_session

dagster-dev: ## Dagster UI on http://localhost:3000 (asset lineage, checks, schedules, sensors)
	$(DAGSTER) dev $(DEFS) -h 127.0.0.1 -p 3000

quality: ## every data-quality + freshness check, standalone (non-zero exit on failure)
	$(RUN) python -m recsys.quality.run

freshness: ## online freshness SLA checks as a Dagster job (RECSYS_CLOCK=... to simulate time passing)
	$(DAGSTER) job execute $(DEFS) -j freshness_checks

serve:   ## build and start Redis + API on :8000
	docker compose up -d --build --wait

latency: ## p50/p95/p99 + throughput against the running API
	$(RUN) python -m recsys.latency.loadgen

demo:    ## end to end on a fresh checkout (batch path via Dagster)
	$(MAKE) setup pipeline parity serve
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
	docker compose --profile stream --profile flink-ui down

flink-ui: ## open the Flink dashboard on http://localhost:8081 until Ctrl-C (REST port is not published otherwise)
	docker compose --profile flink-ui up flink-ui

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

# ---- Phase 4: benchmarks ----------------------------------------------------
BENCH ?= pit,ablation,latency,freshness

benchmarks: ## every experiment (needs `make demo` + `make demo-streaming` once), then README + charts
	$(MAKE) train-streaming stream-up
	docker compose restart app && docker compose up -d --wait app
	$(RUN) python -m benchmarks.run --only $(BENCH)

benchmarks-report: ## regenerate the README RESULTS section and charts from benchmarks/results/
	$(RUN) python -m benchmarks.report

# ---- Phase 3: infrastructure (real AWS + Snowflake) ---------------------------
# Credentials come from your environment: AWS profile/SSO, and SNOWFLAKE_* derived
# from ~/.snowflake/connections.toml at run time (never printed or stored).
# Settings (alert_email, enable_msk, enable_elasticache) go in
# infra/terraform/envs/main/terraform.tfvars (see terraform.tfvars.example).
TF_MAIN := infra/terraform/envs/main
TF_BOOT := infra/terraform/bootstrap
SF_ENV := eval "$$(uv run python scripts/snowflake_env.py)"
TF_KEY := -var "feast_rsa_public_key=$$(grep -v -- '-----' $(CURDIR)/.secrets/feast_rsa_key.pub | tr -d '\n')"

snowflake-keys: ## key pair for the Feast service user (.secrets/, gitignored)
	@mkdir -p .secrets && chmod 700 .secrets
	@[ -f .secrets/feast_rsa_key.p8 ] || ( openssl genrsa 2048 2>/dev/null \
	  | openssl pkcs8 -topk8 -inform PEM -out .secrets/feast_rsa_key.p8 -nocrypt \
	  && openssl rsa -in .secrets/feast_rsa_key.p8 -pubout -out .secrets/feast_rsa_key.pub 2>/dev/null \
	  && chmod 600 .secrets/feast_rsa_key.p8 .secrets/feast_rsa_key.pub )
	@echo "Feast key pair in .secrets/ (the private key never leaves this machine)"

terraform-bootstrap: ## ONE-TIME: creates the S3 state bucket (asks to confirm), then moves state there
	cd $(TF_BOOT) && terraform init -input=false && terraform apply
	cd $(TF_BOOT) && terraform output -raw backend_config > $(CURDIR)/$(TF_MAIN)/backend.hcl
	$(RUN) python scripts/tf_backend.py remote
	cd $(TF_MAIN) && terraform init -input=false -migrate-state -force-copy -backend-config=backend.hcl

terraform-init:
	@$(RUN) python scripts/tf_backend.py auto
	@cd $(TF_MAIN) && if [ -f backend.hcl ]; then terraform init -input=false -backend-config=backend.hcl >/dev/null; \
	  else terraform init -input=false >/dev/null; fi

terraform-plan: snowflake-keys terraform-init ## read-only plan against the real accounts; saves tfplan
	@[ -f $(TF_MAIN)/terraform.tfvars ] || [ -n "$$TF_VAR_alert_email" ] || ( echo "Set the budget alert email first:"; \
	  echo "  cp $(TF_MAIN)/terraform.tfvars.example $(TF_MAIN)/terraform.tfvars   # then edit alert_email"; exit 1 )
	@$(SF_ENV) && cd $(TF_MAIN) && terraform plan -input=false $(TF_KEY) -out=tfplan

terraform-apply: ## YOU run this: applies the saved tfplan (creates billable resources)
	@$(SF_ENV) && cd $(TF_MAIN) && terraform apply tfplan
	$(MAKE) terraform-outputs

terraform-destroy: terraform-init ## tears down everything in envs/main (asks to confirm)
	@$(SF_ENV) && cd $(TF_MAIN) && terraform destroy -input=false $(TF_KEY)

terraform-outputs: ## write .env.cloud (bucket + Snowflake object names) for the cloud profile
	@cd $(TF_MAIN) && terraform output -json | uv run python $(CURDIR)/scripts/write_cloud_env.py > $(CURDIR)/.env.cloud
	@echo "wrote .env.cloud"

demo-snowflake: ## after terraform-apply: load features into Snowflake, compare PIT retrieval with local
	@$(SF_ENV) && set -a && . ./.env.cloud && set +a && $(RUN) python -m recsys.cloud.demo_snowflake

clean:
	docker compose --profile stream --profile flink-ui down -v
	rm -rf data mlruns
