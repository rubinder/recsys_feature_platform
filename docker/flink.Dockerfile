# Flink 2.2 with PyFlink. PyPI has no linux/aarch64 wheel for apache-flink, so it
# builds from source here (about 3 minutes), keeping the image native on Apple Silicon.
# apache-beam (a PyFlink dependency) requires redis<6, hence the older redis client.
FROM flink:2.2.1-java17
USER root
RUN apt-get update -y && apt-get install -y --no-install-recommends python3 python3-venv python3-dev build-essential curl \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/pyflink && /opt/pyflink/bin/pip install --no-cache-dir \
    apache-flink==2.2.1 pydantic==2.14.3 httpx==0.28.1 redis==5.2.1 pyyaml==6.0.3
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar
# Only the import-light streaming modules ship to Flink: contracts + shared logic.
COPY src/recsys/__init__.py /opt/recsys/recsys/__init__.py
COPY src/recsys/contracts.py src/recsys/config.py /opt/recsys/recsys/
COPY config/split.yaml /opt/recsys/config/split.yaml
COPY src/recsys/streaming/__init__.py src/recsys/streaming/logic.py src/recsys/streaming/schema.py src/recsys/streaming/flink_job.py /opt/recsys/recsys/streaming/
# Thread mode (pemja) embeds the interpreter in the JVM; it needs the venv on the path.
ENV PYTHONPATH=/opt/recsys:/opt/pyflink/lib/python3.12/site-packages RECSYS_ROOT=/opt/recsys
USER flink
