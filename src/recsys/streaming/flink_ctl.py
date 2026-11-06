"""Submit / cancel the Flink job via the JobManager (REST + `flink run` in the container)."""

from __future__ import annotations

import argparse
import subprocess
import time

import httpx

REST = "http://localhost:8081"
JOB_NAME = "recsys-user-stream-features"


def running_jobs() -> list[str]:
    jobs = httpx.get(f"{REST}/jobs/overview", timeout=10).json()["jobs"]
    return [j["jid"] for j in jobs if j["state"] in ("RUNNING", "CREATED", "INITIALIZING", "RESTARTING")]


def cancel_all() -> None:
    for jid in running_jobs():
        httpx.patch(f"{REST}/jobs/{jid}", params={"mode": "cancel"}, timeout=10)
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
            tms = httpx.get(f"{REST}/taskmanagers", timeout=5).json()["taskmanagers"]
            if tms and sum(t["freeSlots"] for t in tms) >= 1:
                return
        except httpx.HTTPError:
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
        jobs = httpx.get(f"{REST}/jobs/overview", timeout=10).json()["jobs"]
        if any(j["name"] == JOB_NAME and j["state"] == "RUNNING" for j in jobs):
            print("flink job running:", JOB_NAME)
            return
        time.sleep(0.5)
    raise RuntimeError("flink job did not reach RUNNING")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["submit", "cancel"])
    a = ap.parse_args()
    cancel_all() if a.action == "cancel" else (cancel_all(), submit())
