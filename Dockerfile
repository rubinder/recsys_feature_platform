FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
# uv from PyPI (same pinned version as the lockfile workflow); avoids a ghcr.io dependency at build time
RUN pip install --no-cache-dir uv==0.10.12
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH RECSYS_ROOT=/app OPENBLAS_NUM_THREADS=1
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-default-groups --no-install-project
COPY src ./src
COPY config ./config
COPY benchmarks ./benchmarks
RUN uv sync --frozen --no-default-groups
# Non-root runtime user (uid 10001); the image needs no write access outside mounted data and /tmp.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
USER 10001
EXPOSE 8000
CMD ["uvicorn", "recsys.serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
