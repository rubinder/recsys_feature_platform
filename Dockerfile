FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.10.12 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH RECSYS_ROOT=/app OPENBLAS_NUM_THREADS=1
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-default-groups --no-install-project
COPY src ./src
COPY config ./config
RUN uv sync --frozen --no-default-groups
EXPOSE 8000
CMD ["uvicorn", "recsys.serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
