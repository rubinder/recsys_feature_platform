"""Submit / cancel the Flink job. Everything runs inside the jobmanager container:
the REST port is not published to the host (see docker-compose.yml)."""

from __future__ import annotations

import argparse
import subprocess
import time

import json

JOB_NAME = "recsys-user-stream-features"


def rest(path: str) -> dict:
    """GET Flink's REST API from inside the jobmanager container."""
    out = subprocess.run(["docker", "compose", "exec", "-T", "jobmanager", "curl", "-fs", f"http://localhost:8081{path}"],
                         check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def jobs() -> list[dict]:
    return rest("/jobs/overview")["jobs"]


def running_jobs() -> list[str]:
    return [j["jid"] for j in jobs() if j["state"] in ("RUNNING", "CREATED", "INITIALIZING", "RESTARTING")]


def cancel_all() -> None:
    # REST cancel is disabled (web.cancel.enable: false); use the CLI in the container.
    for jid in running_jobs():
        subprocess.run(["docker", "compose", "exec", "-T", "jobmanager", "flink", "cancel", jid],
                       check=True, capture_output=True)
    for _ in range(100):
        if not running_jobs():
            return
        time.sleep(0.2)


def fresh_taskmanager() -> None:
    """Restart the TaskManager so each job gets a fresh JVM.

    In thread mode PyFlink embeds CPython via pemja, which can initialise only
    once per JVM on Java 17 (it reflects on ClassLoader.loadedLibraryNames,
    which no longer exists): a second Python job in the same TaskManager fails
    with NoSuchFieldException. A clean TaskManager per job avoids it.
    """
    subprocess.run(["docker", "compose", "restart", "taskmanager"], check=True, capture_output=True)
    for _ in range(120):
        try:
            tms = rest("/taskmanagers")["taskmanagers"]
            if tms and sum(t["freeSlots"] for t in tms) >= 1:
                return
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            pass
        time.sleep(0.5)
    raise RuntimeError("taskmanager did not register with the jobmanager")


def submit() -> None:
    fresh_taskmanager()
    subprocess.run(
        ["docker", "compose", "exec", "-T", "jobmanager", "flink", "run", "-d",
         "-py", "/opt/recsys/recsys/streaming/flink_job.py", "-pyfs", "/opt/recsys"],
        check=True,
    )
    for _ in range(150):
        if any(j["name"] == JOB_NAME and j["state"] == "RUNNING" for j in jobs()):
            print("flink job running:", JOB_NAME)
            return
        time.sleep(0.5)
    raise RuntimeError("flink job did not reach RUNNING")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["submit", "cancel"])
    a = ap.parse_args()
    cancel_all() if a.action == "cancel" else (cancel_all(), submit())
