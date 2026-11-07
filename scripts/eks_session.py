"""Temporary EKS session for Phase 5: bring up, deploy, measure, capture, tear down.

    uv run python scripts/eks_session.py --model-uri s3://<mlflow bucket>/.../artifacts/model

Brings up EKS + ElastiCache on top of the Phase 3 base (Terraform), pushes the API
image to ECR, loads features from Snowflake into ElastiCache with an in-cluster
Job, deploys the API behind a load balancer restricted to this machine's IP,
measures latency in-cluster and over the internet, records everything, and then
ALWAYS tears EKS + ElastiCache down again (finally block) and verifies that no
billable resources from the session remain. The Phase 3 base is kept.

Outputs (account id redacted) go to docs/eks-session/: session.log (text),
session.cast (terminal recording), results.json.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "eks-session"
TF = ROOT / "infra" / "terraform" / "envs" / "main"
KUBECONFIG = ROOT / "data" / "eks" / "kubeconfig"
REGION = "us-east-1"
CLUSTER = "recsys-eks"


class Recorder:
    """Writes every command and its output to a text log and an asciicast v2 recording."""

    def __init__(self, redact: list[str]) -> None:
        OUT.mkdir(parents=True, exist_ok=True)
        self.redact = [r for r in redact if r]
        self.t0 = time.monotonic()
        self.log = open(OUT / "session.log", "w")
        self.cast = open(OUT / "session.cast", "w")
        self.cast.write(json.dumps({"version": 2, "width": 120, "height": 36, "timestamp": int(time.time()),
                                    "idle_time_limit": 1.5, "title": "recsys: temporary EKS session"}) + "\n")

    def clean(self, text: str) -> str:
        for r in self.redact:
            text = text.replace(r, "<redacted>")
        return re.sub(r"\b\d{12}\b", "<aws-account-id>", text)

    def emit(self, text: str) -> None:
        text = self.clean(text)
        self.log.write(text)
        self.log.flush()
        self.cast.write(json.dumps([round(time.monotonic() - self.t0, 3), "o", text.replace("\n", "\r\n")]) + "\n")
        self.cast.flush()
        sys.stdout.write(text)
        sys.stdout.flush()

    def note(self, text: str) -> None:
        self.emit(f"\n\x1b[1;36m# {text}\x1b[0m\n")


REC: Recorder


def sh(cmd: str, *, env: dict | None = None, check: bool = True, show: str | None = None, filt: str | None = None,
       stdin: str | None = None, quiet: bool = False, timeout: int = 3600) -> str:
    """Run a shell command, streaming (optionally filtered) output into the recording."""
    full_env = {**os.environ, "KUBECONFIG": str(KUBECONFIG), "AWS_DEFAULT_REGION": REGION, **(env or {})}
    if not quiet:
        REC.emit(f"\x1b[1;32m$\x1b[0m {show or cmd}\n")
    p = subprocess.Popen(cmd, shell=True, cwd=ROOT, env=full_env, text=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, stdin=subprocess.PIPE if stdin else None)
    if stdin:
        p.stdin.write(stdin)
        p.stdin.close()
    out = []
    deadline = time.monotonic() + timeout
    for line in p.stdout:
        out.append(line)
        if not quiet and (filt is None or re.search(filt, line)):
            REC.emit(line)
        if time.monotonic() > deadline:
            p.kill()
            raise TimeoutError(cmd)
    rc = p.wait()
    if check and rc != 0:
        tail = "".join(out[-25:])
        if quiet:
            REC.emit(tail)
        raise RuntimeError(f"command failed ({rc}): {show or cmd}")
    return "".join(out)


def tf_vars(eks: bool, ip: str) -> str:
    key = (ROOT / ".secrets" / "feast_rsa_key.pub").read_text()
    key = "".join(line for line in key.splitlines() if "-----" not in line)
    flag = "true" if eks else "false"
    return (f'-var "feast_rsa_public_key={key}" -var enable_eks={flag} -var enable_elasticache={flag} '
            f"-var 'eks_public_access_cidrs=[\"{ip}/32\"]'")


def terraform(eks: bool, ip: str) -> None:
    sh(f"terraform -chdir={TF} apply -input=false -auto-approve -no-color {tf_vars(eks, ip)}",
       show=f"terraform apply -var enable_eks={str(eks).lower()} -var enable_elasticache={str(eks).lower()} (Phase 3 base unchanged)",
       filt=r"(Creation complete|Destruction complete|Apply complete|Error|Modifications complete)", timeout=3600)


def wait_http(url: str, minutes: int = 10) -> None:
    for _ in range(minutes * 12):
        try:
            if subprocess.run(["curl", "-sf", "-m", "5", url], capture_output=True).returncode == 0:
                return
        except Exception:
            pass
        time.sleep(5)
    raise TimeoutError(url)


LOADTEST = r'''
import asyncio, json, random, time
import httpx
URL = "http://recsys-api.default.svc.cluster.local"
async def run(n, c, client, users):
    sem = asyncio.Semaphore(c); lat = []
    async def one(u):
        async with sem:
            t = time.perf_counter(); r = await client.get(f"/recommendations/{u}", params={"log_impression": "false"})
            lat.append((time.perf_counter() - t) * 1000); assert r.status_code == 200
    t0 = time.perf_counter(); await asyncio.gather(*(one(u) for u in users[:n])); wall = time.perf_counter() - t0
    lat.sort(); q = lambda p: lat[min(len(lat) - 1, int(p * len(lat)))]
    return {"concurrency": c, "requests": n, "p50_ms": q(0.5), "p95_ms": q(0.95), "p99_ms": q(0.99), "rps": n / wall}
async def main():
    rnd = random.Random(42); users = [rnd.randint(1, 6040) for _ in range(3000)]
    async with httpx.AsyncClient(base_url=URL, timeout=10) as client:
        for _ in range(100): await client.get("/recommendations/6040", params={"log_impression": "false"})  # warm-up
        await client.post("/latency/reset")
        res = [await run(1500, 1, client, users), await run(3000, 8, client, users)]
        server = (await client.get("/latency")).json()
    print("LOADTEST_RESULT " + json.dumps({"runs": res, "server_stages_ms": server}))
asyncio.run(main())
'''


def diagnostics() -> None:
    """Pod status, events and logs into the session log, so a failure leaves evidence before teardown."""
    REC.note("Diagnostics (captured before teardown)")
    for cmd in ("kubectl get pods,jobs -o wide", "kubectl get events --sort-by=.lastTimestamp | tail -30",
                "kubectl describe pods -l job-name=recsys-materialize | tail -40",
                "kubectl logs job/recsys-materialize --tail=60", "kubectl logs deploy/recsys-api --tail=40"):
        sh(cmd, check=False, timeout=120)


def wait_job(name: str, minutes: int) -> None:
    """Wait for a Job, streaming its pod's new log lines; fail fast on errors instead of a long timeout."""
    seen = 0
    for _ in range(minutes * 6):
        st = json.loads(sh(f"kubectl get job/{name} -o json", quiet=True, check=False) or "{}").get("status", {})
        logs = sh(f"kubectl logs job/{name} --tail=200", quiet=True, check=False).splitlines()
        for line in logs[seen:]:
            if re.search(r"materialized|Error|error|Traceback|LOADTEST", line):
                REC.emit(f"  [{name}] {line}\n")
        seen = max(seen, len(logs))
        if st.get("succeeded"):
            return
        pods = sh(f"kubectl get pods -l job-name={name} -o jsonpath='{{.items[*].status.containerStatuses[*].state}}'",
                  quiet=True, check=False)
        if st.get("failed") or re.search(r"ImagePullBackOff|ErrImagePull|CrashLoopBackOff|CreateContainerConfigError", pods):
            raise RuntimeError(f"job {name} failed: {pods[:300]}")
        time.sleep(10)
    raise TimeoutError(f"job {name} did not finish in {minutes} min")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-uri", required=True, help="s3:// URI of a trained model/ artifact directory")
    ap.add_argument("--hold-minutes", type=float, default=4, help="time the endpoint stays up for captures")
    args = ap.parse_args()

    # Self-refreshing AWS credentials: a profile whose credential_process asks the CLI's
    # login/SSO session each time. Exported credentials last ~15 minutes, shorter than an
    # EKS bring-up (the first session lost its credentials mid-apply).
    cfg = ROOT / "data" / "eks" / "aws-config"
    cfg.parent.mkdir(parents=True, exist_ok=True)
    cfg.write_text("[profile recsys-session]\nregion = us-east-1\ncredential_process = env -u AWS_PROFILE "
                   f"AWS_CONFIG_FILE={Path.home() / '.aws' / 'config'} aws configure export-credentials --format process\n")
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_CREDENTIAL_EXPIRATION"):
        os.environ.pop(k, None)
    os.environ.update({"AWS_CONFIG_FILE": str(cfg), "AWS_PROFILE": "recsys-session"})
    sf = subprocess.run([sys.executable, str(ROOT / "scripts" / "snowflake_env.py")], capture_output=True, text=True,
                        check=True).stdout
    for line in sf.splitlines():
        k, _, v = line.removeprefix("export ").partition("=")
        os.environ[k] = shlex.split(v)[0] if v else v
    cloud = dict(line.split("=", 1) for line in (ROOT / ".env.cloud").read_text().splitlines() if "=" in line)
    ip = subprocess.run(["curl", "-s", "https://checkip.amazonaws.com"], capture_output=True, text=True).stdout.strip()
    secrets = [os.environ.get("SNOWFLAKE_TOKEN", ""), os.environ.get("SNOWFLAKE_ORGANIZATION_NAME", ""),
               os.environ.get("SNOWFLAKE_ACCOUNT_NAME", ""), ip]
    global REC
    REC = Recorder(secrets)
    results: dict = {"started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    t_start = time.monotonic()
    KUBECONFIG.parent.mkdir(parents=True, exist_ok=True)
    try:
        REC.note("1/7 Bring up EKS (ARM node group) + ElastiCache in the Phase 3 VPC")
        t = time.monotonic()
        terraform(True, ip)
        results["bring_up_minutes"] = round((time.monotonic() - t) / 60, 1)
        out = json.loads(sh(f"terraform -chdir={TF} output -json", quiet=True))
        ecr, redis_host = out["ecr_repository_url"]["value"], out["redis_endpoint"]["value"]
        token = sh(f"terraform -chdir={TF} output -raw redis_auth_token", quiet=True).strip()
        REC.redact.append(token)

        REC.note("2/7 Build the API image (arm64) and push it to ECR")
        tag = sh("git rev-parse --short HEAD", quiet=True).strip()
        sh(f"docker build -q -t {ecr}:{tag} .", show=f"docker build -t <ecr>/recsys-api:{tag} .", filt=r"sha256|ERROR")
        sh(f"aws ecr get-login-password | docker login --username AWS --password-stdin {ecr.split('/')[0]}",
           show="aws ecr get-login-password | docker login ... (ECR)", filt=r"Login|Error")
        sh(f"docker push -q {ecr}:{tag}", show=f"docker push <ecr>/recsys-api:{tag}")

        REC.note("3/7 Cluster access and the Feast registry in S3")
        sh(f"aws eks update-kubeconfig --name {CLUSTER} --region {REGION} --kubeconfig {KUBECONFIG}",
           show=f"aws eks update-kubeconfig --name {CLUSTER}", filt=r"Added|Updated|error")
        sh("kubectl get nodes -o wide", filt=r".")
        registry = f"s3://{cloud['RECSYS_LAKE_BUCKET']}/feast/registry_cloud.db"
        sh(f"{sys.executable} -m recsys.features.apply", env={**cloud, "RECSYS_PROFILE": "cloud",
           "RECSYS_FEAST_REGISTRY": registry}, show="RECSYS_PROFILE=cloud feast apply  (registry -> s3://<lake>/feast/)",
           filt=r"applied|Error")

        REC.note("4/7 Secrets + config, then deploy: materialize Job (Snowflake -> ElastiCache) and the API")
        cm = {k: v for k, v in cloud.items() if k.startswith("RECSYS_") and k != "RECSYS_SF_PRIVATE_KEY_PATH"}
        cm.update({"SNOWFLAKE_ORGANIZATION_NAME": os.environ["SNOWFLAKE_ORGANIZATION_NAME"],
                   "SNOWFLAKE_ACCOUNT_NAME": os.environ["SNOWFLAKE_ACCOUNT_NAME"], "RECSYS_FEAST_REGISTRY": registry,
                   "AWS_REGION": REGION, "AWS_DEFAULT_REGION": REGION})
        manifests = {
            "apiVersion": "v1", "kind": "List", "items": [
                {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "recsys-cloud"}, "data": cm},
                {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "recsys-redis"}, "type": "Opaque",
                 "stringData": {"connection": f"{redis_host}:6379,ssl=true,password={token}"}},
                {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "recsys-snowflake-key"}, "type": "Opaque",
                 "stringData": {"feast_rsa_key.p8": (ROOT / ".secrets" / "feast_rsa_key.p8").read_text()}},
            ]}
        sh("kubectl apply -f -", stdin=json.dumps(manifests), show="kubectl apply -f -  (ConfigMap recsys-cloud, Secrets recsys-redis, recsys-snowflake-key)")
        rendered = sh(
            f"docker run --rm -v {ROOT}/deploy/helm:/charts alpine/helm:3.17.3 template recsys /charts/recsys "
            f"-f /charts/recsys/values-eks.yaml --set image.repository={ecr},image.tag={tag},image.pullPolicy=Always "
            f"--set-string 'api.modelVariants=champion={args.model_uri}' --set 'service.sourceRanges={{{ip}/32}}'",
            quiet=True)
        sh("kubectl apply -f -", stdin=rendered, show="helm template recsys deploy/helm/recsys -f values-eks.yaml | kubectl apply -f -")
        t = time.monotonic()
        REC.emit("$ kubectl logs -f job/recsys-materialize  (Snowflake -> ElastiCache)\n")
        wait_job("recsys-materialize", 15)
        results["materialize_seconds"] = round(time.monotonic() - t)
        sh("kubectl rollout status deploy/recsys-api --timeout=10m", timeout=700)
        sh("kubectl get pods,svc -o wide", filt=r".")

        REC.note("5/7 Measure latency inside the VPC (in-cluster load test Job)")
        job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "recsys-loadtest"}, "spec": {
            "backoffLimit": 0, "template": {"spec": {"restartPolicy": "Never",
                "securityContext": {"runAsNonRoot": True, "runAsUser": 10001},
                "containers": [{"name": "loadtest", "image": f"{ecr}:{tag}", "command": ["python", "-c", LOADTEST],
                                "securityContext": {"allowPrivilegeEscalation": False}}]}}}}
        sh("kubectl apply -f -", stdin=json.dumps(job), show="kubectl apply -f loadtest-job  (3,000 requests from inside the cluster)")
        wait_job("recsys-loadtest", 10)
        lt = sh("kubectl logs job/recsys-loadtest", quiet=True)
        results["in_cluster"] = json.loads(lt.split("LOADTEST_RESULT ", 1)[1])
        for r in results["in_cluster"]["runs"]:
            REC.emit(f"  in-cluster c={r['concurrency']}: p50 {r['p50_ms']:.1f} ms  p95 {r['p95_ms']:.1f} ms  "
                     f"p99 {r['p99_ms']:.1f} ms  {r['rps']:.0f} req/s\n")

        REC.note("6/7 Public endpoint (load balancer, restricted to this machine's IP)")
        host = ""
        for _ in range(60):
            host = sh("kubectl get svc recsys-api -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'", quiet=True).strip()
            if host:
                break
            time.sleep(5)
        wait_http(f"http://{host}/health")
        results["endpoint"] = f"http://{host}"
        sh(f"curl -s http://{host}/health", show="curl http://<elb>/health", filt=r".")
        REC.emit("\n")
        sh(f"curl -s 'http://{host}/recommendations/6040?log_impression=false' | python3 -m json.tool | head -30",
           show="curl http://<elb>/recommendations/6040", filt=r".")
        (OUT / "endpoint.txt").write_text(f"http://{host}\n")
        REC.note(f"Endpoint stays up {args.hold_minutes:g} min for screen captures")
        hold_until = time.monotonic() + args.hold_minutes * 60
        while time.monotonic() < hold_until and not (OUT / "captures.done").exists():
            time.sleep(5)
    except Exception as exc:
        results["error"] = str(exc)[:500]
        try:
            diagnostics()
        except Exception:
            pass
        raise
    finally:
        REC.note("7/7 Tear down EKS + ElastiCache (always runs), keep the Phase 3 base")
        sh("kubectl delete svc recsys-api --ignore-not-found --wait=true --timeout=5m", check=False,
           show="kubectl delete svc recsys-api  (removes the AWS load balancer first)")
        for _ in range(36):  # the in-tree controller deletes the ELB asynchronously
            left = sh("aws elb describe-load-balancers --query 'LoadBalancerDescriptions[].LoadBalancerName' --output text",
                      quiet=True, check=False).split()
            tagged = [n for n in left if "recsys" in sh(f"aws elb describe-tags --load-balancer-names {n} --output text",
                                                        quiet=True, check=False)]
            if not tagged:
                break
            time.sleep(5)
        t = time.monotonic()
        terraform(False, ip)
        results["tear_down_minutes"] = round((time.monotonic() - t) / 60, 1)
        REC.note("Verify nothing billable from the session remains")
        # Only this project's resources (the account may hold unrelated ones).
        checks = {
            "eks_clusters": "aws eks list-clusters --query \"clusters[?contains(@, 'recsys')]\" --output text",
            "elasticache": "aws elasticache describe-replication-groups --query \"ReplicationGroups[?contains(ReplicationGroupId, 'recsys')].ReplicationGroupId\" --output text",
            "ec2_running": "aws ec2 describe-instances --filters Name=instance-state-name,Values=running,pending "
                           "Name=tag:eks:cluster-name,Values=recsys-eks --query 'Reservations[].Instances[].InstanceId' --output text",
            "k8s_load_balancers": "aws resourcegroupstaggingapi get-resources --resource-type-filters elasticloadbalancing:loadbalancer "
                                  "--tag-filters Key=kubernetes.io/cluster/recsys-eks --query 'ResourceTagMappingList[].ResourceARN' --output text",
            "ecr_repo": "aws ecr describe-repositories --query \"repositories[?repositoryName=='recsys-api'].repositoryName\" --output text",
        }
        verify = {k: sh(c, show=c, check=False).strip() for k, c in checks.items()}
        results["verify"] = verify
        results["clean"] = not any(verify.values())
        REC.emit(f"\nleftover billable resources: {'none' if results['clean'] else verify}\n")
        results["total_minutes"] = round((time.monotonic() - t_start) / 60, 1)
        (OUT / "results.json").write_text(REC.clean(json.dumps(results, indent=2)) + "\n")
        REC.emit(f"session wall time {results['total_minutes']} min\n")


if __name__ == "__main__":
    main()
