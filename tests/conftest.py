from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = REPO_ROOT / "docker" / "compose.remote-langgraph.yml"
PROJECT = os.environ.get("AMAZEGRAPH_COMPOSE_PROJECT", "amazegraph-test")

ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://localhost:8011")
JAEGER_URL = os.environ.get("JAEGER_URL", "http://localhost:16696")
RESEARCH_HEALTH_URL = os.environ.get("RESEARCH_HEALTH_URL", "http://localhost:9012/healthz")
WRITER_HEALTH_URL = os.environ.get("WRITER_HEALTH_URL", "http://localhost:9013/healthz")
# Sprint 2 services — NOTE: these four services do NOT publish host ports in
# compose.remote-langgraph.yml, so the defaults below only work if the ports
# are explicitly published (e.g. in a dev override file).  They are kept here
# for manual debugging convenience; do not use them in automated assertions
# without first confirming the ports are published.
LLM_TOOL_HEALTH_URL = os.environ.get("LLM_TOOL_HEALTH_URL", "http://localhost:9004/healthz")
AUDIT_HEALTH_URL = os.environ.get("AUDIT_HEALTH_URL", "http://localhost:9005/healthz")
RESEARCH_A_HEALTH_URL = os.environ.get("RESEARCH_A_HEALTH_URL", "http://localhost:9006/healthz")
RESEARCH_B_HEALTH_URL = os.environ.get("RESEARCH_B_HEALTH_URL", "http://localhost:9007/healthz")


def _compose(*args: str, capture: bool = True, check: bool = False, timeout: int = 600) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE), *args]
    return subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        check=check,
        timeout=timeout,
    )


def _wait_for_url(url: str, timeout: float = 90.0) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = httpx.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except Exception as exc:
            last_err = exc
        time.sleep(1.0)
    raise RuntimeError(f"timeout waiting for {url}: {last_err}")


@pytest.fixture(scope="session", autouse=True)
def compose_stack():
    if os.environ.get("AMAZEGRAPH_SKIP_COMPOSE") == "1":
        yield
        return

    _compose("down", "-v", capture=True)
    _compose(
        "up",
        "-d",
        "--build",
        # NOTE: "jaeger" was removed — Jaeger is now embedded inside the
        # orchestrator container (post-S1 polish). Starting it separately
        # would fail with "no such service: jaeger".
        "orchestrator",
        "a2a-research",
        "a2a-writer",
        check=True,
        timeout=900,
    )
    _wait_for_url(f"{ORCHESTRATOR_URL}/health")
    _wait_for_url(RESEARCH_HEALTH_URL)
    _wait_for_url(WRITER_HEALTH_URL)
    yield
    _compose("logs", "--no-color", "--tail", "200", capture=True)
    _compose("down", "-v", capture=True)


@pytest.fixture
def orchestrator_url() -> str:
    return ORCHESTRATOR_URL


@pytest.fixture
def jaeger_url() -> str:
    return JAEGER_URL


@pytest.fixture
def run_main_langgraph():
    def _run(extra_env: dict | None = None, run_id: str | None = None) -> subprocess.CompletedProcess:
        env_args: list[str] = []
        if extra_env:
            for k, v in extra_env.items():
                env_args.extend(["-e", f"{k}={v}"])
        cmd = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE), "run", "--rm", *env_args, "main-langgraph"]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=180)

    return _run


@pytest.fixture
def temporarily_unregister_node():
    saved: list[dict[str, str]] = []

    def _unreg(graph_id: str, node_name: str) -> None:
        r = httpx.get(
            f"{ORCHESTRATOR_URL}/resolve/node/{graph_id}/{node_name}",
            timeout=5.0,
        )
        if r.status_code != 200:
            return
        endpoint = r.json()["endpoint"]
        body = {"graph_id": graph_id, "node_name": node_name, "endpoint": endpoint}
        httpx.request(
            "DELETE",
            f"{ORCHESTRATOR_URL}/register/node",
            json=body,
            timeout=5.0,
        )
        saved.append(body)

    yield _unreg

    for body in saved:
        try:
            httpx.post(
                f"{ORCHESTRATOR_URL}/register/node",
                json=body,
                timeout=5.0,
            )
        except Exception:
            pass


@pytest.fixture(scope="session")
def sprint2_demo(compose_stack) -> subprocess.CompletedProcess:
    """Run the Sprint-2 all-scenario demo once; shared across ST-RLG-7..13.

    All seven scenarios (S1-S6) execute in a single container run so each
    test can query the orchestrator using the fixed run IDs: run-s1..run-s6.
    """
    cmd = [
        "docker", "compose",
        "-p", PROJECT,
        "-f", str(COMPOSE_FILE),
        "run", "--rm", "main-langgraph",
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


@pytest.fixture
def research_with_env():
    container_name = f"{PROJECT}-a2a-research-debug"
    image = f"{PROJECT}-a2a-research"
    network = f"{PROJECT}_default"
    debug_endpoint = f"http://{container_name}:9002/invoke"
    saved: dict[str, str] | None = None

    def _start(extra_env: dict[str, str]) -> None:
        nonlocal saved
        r = httpx.get(
            f"{ORCHESTRATOR_URL}/resolve/node/demo_graph_v1/research",
            timeout=5.0,
        )
        if r.status_code == 200:
            saved = {
                "graph_id": "demo_graph_v1",
                "node_name": "research",
                "endpoint": r.json()["endpoint"],
            }

        env_pairs: list[str] = []
        for k, v in extra_env.items():
            env_pairs.extend(["-e", f"{k}={v}"])
        env_pairs.extend([
            "-e", "AMAZE_ORCHESTRATOR_URL=http://orchestrator:8001",
            "-e", f"A2A_NODE_PUBLIC_ENDPOINT={debug_endpoint}",
            "-e", "A2A_NODE_HOST=0.0.0.0",
            "-e", "A2A_NODE_PORT=9002",
            "-e", "OTEL_EXPORTER_OTLP_ENDPOINT=http://orchestrator:4317",
        ])
        cmd = [
            "docker", "run", "-d", "--name", container_name,
            "--network", network, *env_pairs, image,
        ]
        subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=120)

        deadline = time.time() + 30.0
        while time.time() < deadline:
            try:
                rr = httpx.get(
                    f"{ORCHESTRATOR_URL}/resolve/node/demo_graph_v1/research",
                    timeout=2.0,
                )
                if rr.status_code == 200 and rr.json().get("endpoint") == debug_endpoint:
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError("debug research container did not re-register endpoint within 30s")

    yield _start

    subprocess.run(
        ["docker", "rm", "-f", container_name],
        capture_output=True,
        text=True,
    )
    if saved is not None:
        try:
            httpx.post(
                f"{ORCHESTRATOR_URL}/register/node",
                json=saved,
                timeout=5.0,
            )
        except Exception:
            pass
