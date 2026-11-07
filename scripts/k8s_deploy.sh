#!/usr/bin/env bash
# Optional K8S=1 path: kind cluster + Helm release of the serving API.
set -euo pipefail
for tool in kind helm kubectl docker; do
  command -v "$tool" >/dev/null || { echo "missing $tool (brew install kind helm kubectl)"; exit 1; }
done
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if ! kind get clusters | grep -qx recsys; then
  sed "s|HOST_ROOT|$ROOT|g" "$ROOT/deploy/kind.yaml" > "$ROOT/data/kind.rendered.yaml"
  kind create cluster --name recsys --config "$ROOT/data/kind.rendered.yaml"
fi
docker build -t recsys-api:local "$ROOT"
kind load docker-image recsys-api:local --name recsys
helm upgrade --install recsys "$ROOT/deploy/helm/recsys" --set hostRoot="$ROOT" --wait --timeout 10m
kubectl rollout status deploy/recsys-api
echo "API on http://localhost:8080 (kind nodePort). Load test: make loadtest K8S=1"
