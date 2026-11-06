"""Feast feature server (push + online reads) built from the code-defined store."""

from feast.feature_server import get_app

from recsys.feature_repo.store import get_store

app = get_app(get_store(), registry_ttl_sec=60)
