#!/usr/bin/env bash
# stack-sprint4.sh — stop all aMaze containers, start only what Sprint 4 tests need.
#
# Brings up:  orchestrator  a2a-command
# Sufficient for: ST-RLG-19..22 (test_sprint4.py)
# S1-S10 will SKIP gracefully (no research/writer nodes required).
#
# Usage:
#   ./scripts/stack-sprint4.sh          # start
#   ./scripts/stack-sprint4.sh down     # stop everything

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker/compose.remote-langgraph.yml"
PROJECT="amazegraph-test"
DC="docker compose -p $PROJECT -f $COMPOSE_FILE"

# ── Tear-down ─────────────────────────────────────────────────────────────────

echo "==> Stopping all $PROJECT containers..."
$DC down -v --remove-orphans 2>/dev/null || true

# Kill stray aMaze Python processes that may be holding ports
echo "==> Clearing stray aMaze processes on ports 9011 9012 9013..."
for PORT in 9011 9012 9013; do
    PID=$(ss -tlnp "sport = :$PORT" 2>/dev/null | grep -oP '(?<=pid=)\d+' | head -1 || true)
    if [ -n "$PID" ]; then
        CMD=$(ps -p "$PID" -o cmd= 2>/dev/null || echo "?")
        echo "   killing pid=$PID ($CMD) on :$PORT"
        kill "$PID" 2>/dev/null || true
    fi
done

if [ "${1:-}" = "down" ]; then
    echo "==> Stack stopped."
    exit 0
fi

# ── Start only what the sprint4 tests need ────────────────────────────────────

echo ""
# Build main-langgraph image separately (one-shot; not started as a daemon)
echo ""
echo "==> Building main-langgraph image..."
$DC build main-langgraph

echo ""
echo "==> Building and starting: orchestrator, a2a-writer, a2a-command..."
$DC up -d --build orchestrator a2a-writer a2a-command

# ── Wait for health ───────────────────────────────────────────────────────────

echo ""
echo "==> Waiting for services..."

wait_url() {
    local URL="$1" LABEL="$2" DEADLINE=$((SECONDS + 90))
    printf "   %-36s" "$LABEL"
    until curl -sf "$URL" > /dev/null 2>&1; do
        if [ $SECONDS -ge $DEADLINE ]; then echo " TIMEOUT"; exit 1; fi
        printf "."; sleep 2
    done
    echo " ok"
}

wait_url "http://localhost:8011/health"   "orchestrator"
wait_url "http://localhost:9013/healthz" "a2a-writer"

# a2a-command has no host port; poll via orchestrator
printf "   %-36s" "a2a-command (via orchestrator)"
DEADLINE=$((SECONDS + 90))
until curl -sf "http://localhost:8011/resolve/node/demo_graph_v1/command" > /dev/null 2>&1; do
    if [ $SECONDS -ge $DEADLINE ]; then echo " TIMEOUT"; exit 1; fi
    printf "."; sleep 2
done
echo " ok"

# ── Done ─────────────────────────────────────────────────────────────────────

echo ""
echo "==> Stack ready. Run Sprint 4 tests:"
echo ""
echo "    AMAZEGRAPH_SKIP_COMPOSE=1 \\"
echo "    ORCHESTRATOR_URL=http://localhost:8011 \\"
echo "    JAEGER_URL=http://localhost:16696 \\"
echo "    /home/ubuntu/venv/bin/python -m pytest tests/system/test_sprint4.py -v"
echo ""
