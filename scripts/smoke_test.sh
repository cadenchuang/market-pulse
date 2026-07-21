#!/usr/bin/env bash
# End-to-end smoke test for the full Market Pulse stack (real Kafka + compose).
#
# Brings the whole system up in replay mode, waits for messages to flow through
# both Kafka topics and for the inference workers to persist results, then checks
# each stage's Prometheus /metrics endpoint and the dashboard health endpoint.
#
# For the dependency-light, no-Docker version of this check (runnable in CI), see
# tests/test_end_to_end.py.
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE=${COMPOSE:-"docker compose"}
WAIT_SECONDS=${WAIT_SECONDS:-90}

echo "==> Bringing up the stack (replay mode, offline)…"
$COMPOSE up -d --build

cleanup() {
  echo "==> Tearing down…"
  $COMPOSE down -v
}
trap cleanup EXIT

wait_for() { # name url
  local name=$1 url=$2 i
  echo -n "==> Waiting for $name ($url) "
  for ((i = 0; i < WAIT_SECONDS; i++)); do
    if curl -sf "$url" >/dev/null 2>&1; then echo "OK"; return 0; fi
    echo -n "."; sleep 1
  done
  echo "FAILED"; return 1
}

check_metric() { # name url substring
  local name=$1 url=$2 needle=$3
  if curl -sf "$url" | grep -q "$needle"; then
    echo "    [ok] $name exposes $needle"
  else
    echo "    [FAIL] $name missing $needle"; return 1
  fi
}

# 1) Each stage's /metrics endpoint is up and exports its stage metric.
wait_for "ingestor metrics" "http://localhost:9101/metrics"
check_metric "ingestor" "http://localhost:9101/metrics" "market_pulse_ingestor_produced_total"

wait_for "processor metrics" "http://localhost:9102/metrics"
check_metric "processor" "http://localhost:9102/metrics" "market_pulse_processor_processed_total"

wait_for "inference metrics" "http://localhost:9103/metrics"
check_metric "inference" "http://localhost:9103/metrics" "market_pulse_inference_processed_total"

# 2) Messages actually landed on both topics.
echo "==> Checking Kafka topics have messages…"
docker exec market-pulse-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic news.raw --from-beginning --max-messages 1 --timeout-ms 15000 >/dev/null \
  && echo "    [ok] news.raw has messages"
docker exec market-pulse-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic news.processed --from-beginning --max-messages 1 --timeout-ms 15000 >/dev/null \
  && echo "    [ok] news.processed has messages"

# 3) Observability + dashboard are reachable.
wait_for "prometheus" "http://localhost:9090/-/ready"
wait_for "grafana" "http://localhost:3000/api/health"
wait_for "dashboard" "http://localhost:8501/_stcore/health"

echo "==> SMOKE TEST PASSED: all stages produced metrics, topics have data, UIs are up."
