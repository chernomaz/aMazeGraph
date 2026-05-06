from __future__ import annotations

import os

import uvicorn

from services.orchestrator.api import app, setup_logging

ORCHESTRATOR_HOST = os.environ.get("ORCHESTRATOR_HOST", "0.0.0.0")
ORCHESTRATOR_PORT = int(os.environ.get("ORCHESTRATOR_PORT", "8001"))

setup_logging("orchestrator")

if __name__ == "__main__":
    uvicorn.run(app, host=ORCHESTRATOR_HOST, port=ORCHESTRATOR_PORT, log_config=None)
