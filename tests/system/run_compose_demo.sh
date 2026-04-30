#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker/compose.remote-langgraph.yml"

cd "$REPO_ROOT"
exec docker compose -f "$COMPOSE_FILE" up --build --abort-on-container-exit --exit-code-from main-langgraph
