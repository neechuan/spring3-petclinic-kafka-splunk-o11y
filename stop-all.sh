#!/usr/bin/env bash
#
# Stop the Kafka PetClinic stack - the reverse of run-all.sh. Stop everything
# or one service at a time:
#   frontend   Stop the frontend (whatever is listening on 8080)
#   backend    Stop the backend  (whatever is listening on 8081)
#   kafka      Stop and remove the Kafka broker container
#   apps       Stop frontend and backend (no broker)
#   all        Stop frontend, backend and kafka (default)
#
# Services are stopped in the reverse of run-all.sh's start order:
# frontend, then backend, then kafka.
set -euo pipefail
cd "$(dirname "$0")"

KAFKA_CONTAINER="${KAFKA_CONTAINER:-petclinic-kafka}"

usage() {
  cat <<'EOF'
Usage: ./stop-all.sh [target ...]

Targets:
  kafka      Stop and remove the Apache Kafka broker container
  backend    Stop the backend  (listening on 8081)
  frontend   Stop the frontend (listening on 8080)
  apps       Stop frontend and backend (no broker)
  all        Stop frontend, backend and kafka (default)

Examples:
  ./stop-all.sh                # stop everything
  ./stop-all.sh kafka          # just the broker
  ./stop-all.sh frontend       # just the frontend
  ./stop-all.sh apps           # frontend + backend
EOF
}

# ---- parse targets ---------------------------------------------------------
want_solace=0 want_backend=0 want_frontend=0
targets=("$@")
[ ${#targets[@]} -eq 0 ] && targets=(all)
for t in "${targets[@]}"; do
  case "$t" in
    all)      want_solace=1; want_backend=1; want_frontend=1 ;;
    apps)     want_backend=1; want_frontend=1 ;;
    kafka)    want_solace=1 ;;
    backend)  want_backend=1 ;;
    frontend) want_frontend=1 ;;
    -h|--help|help) usage; exit 0 ;;
    *) echo "error: unknown target '$t'" >&2; usage; exit 1 ;;
  esac
done

# stop_port <name> <port> - stop whatever process is listening on <port>
stop_port() {
  local name=$1 port=$2 pids i
  pids=$(lsof -ti "tcp:$port" -sTCP:LISTEN 2>/dev/null || true)
  if [ -z "$pids" ]; then
    echo "$name: not running (nothing listening on $port)."
    return 0
  fi
  echo "Stopping $name (pid $(echo "$pids" | tr '\n' ' ')on port $port)..."
  kill $pids 2>/dev/null || true
  for i in $(seq 1 10); do
    pids=$(lsof -ti "tcp:$port" -sTCP:LISTEN 2>/dev/null || true)
    [ -z "$pids" ] && { echo "$name stopped."; return 0; }
    sleep 1
  done
  echo "$name did not stop gracefully; forcing..."
  kill -9 $pids 2>/dev/null || true
  echo "$name stopped."
}

stop_kafka() {
  if podman container exists "$KAFKA_CONTAINER" 2>/dev/null; then
    echo "Stopping Kafka broker '$KAFKA_CONTAINER'..."
    podman rm -f "$KAFKA_CONTAINER" >/dev/null
    echo "Kafka broker stopped."
  else
    echo "kafka: not running."
  fi
}

# ---- act in reverse order: frontend, backend, solace -----------------------
[ $want_frontend -eq 1 ] && stop_port frontend 8080
[ $want_backend -eq 1 ]  && stop_port backend  8081
[ $want_solace -eq 1 ]   && stop_kafka
