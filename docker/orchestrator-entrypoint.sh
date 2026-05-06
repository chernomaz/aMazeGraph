#!/bin/sh
set -e

/opt/redis-stack/bin/redis-stack-server --daemonize yes --bind 0.0.0.0 --port 6379 \
  --save "" --appendonly no --protected-mode no

for i in $(seq 1 30); do
  if /opt/redis-stack/bin/redis-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

COLLECTOR_OTLP_ENABLED=true /usr/local/bin/jaeger-all-in-one >/proc/1/fd/1 2>&1 &

for i in $(seq 1 50); do
  if wget -q -O- http://127.0.0.1:14269/ >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

exec python -m services.orchestrator.main
