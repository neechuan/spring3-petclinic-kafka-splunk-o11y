#!/usr/bin/env bash
#
# Start the Kafka PetClinic stack, all together or one service at a time:
#   kafka      Apache Kafka broker  (detached container, KRaft mode)
#   backend    persistence + Kafka replier   (http://localhost:8081)
#   frontend   UI + Kafka requestor          (http://localhost:8080)
#   all        kafka + backend + frontend   (default)
#
# A single requested app runs in the foreground with live logs (Ctrl+C stops
# it). Multiple apps run in the background (logs in ./logs) and are stopped
# together with Ctrl+C. The broker always runs detached - stop it with:
#   ./stop-all.sh kafka
set -euo pipefail
cd "$(dirname "$0")"

# Load local config/secrets from .env if present (keeps SPLUNK_ACCESS_TOKEN out
# of this script and out of git - see .gitignore). set -a exports every value.
if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  . ./.env
  set +a
fi

usage() {
  cat <<'EOF'
Usage: ./run-all.sh [target ...]

Targets:
  kafka      Start the Apache Kafka broker (detached container, KRaft mode)
  backend    Start the backend  (persistence + replier, http://localhost:8081)
  frontend   Start the frontend (UI + requestor,        http://localhost:8080)
  apps       Start backend then frontend (no broker)
  all        Start kafka, backend and frontend (default)

Examples:
  ./run-all.sh                 # start everything
  ./run-all.sh kafka           # just the broker
  ./run-all.sh backend         # just the backend (live logs; Ctrl+C to stop)
  ./run-all.sh apps            # backend then frontend
  ./run-all.sh kafka backend   # broker + backend
EOF
}

# Prefer the bundled Maven wrapper if it is configured, otherwise system mvn.
if [ -x ./mvnw ] && [ -f .mvn/wrapper/maven-wrapper.properties ]; then
  MVN=./mvnw
elif command -v mvn >/dev/null 2>&1; then
  MVN=mvn
else
  echo "error: no working ./mvnw wrapper and 'mvn' is not on PATH." >&2
  exit 1
fi

# ---- parse targets ---------------------------------------------------------
want_kafka=0 want_backend=0 want_frontend=0
targets=("$@")
[ ${#targets[@]} -eq 0 ] && targets=(all)
for t in "${targets[@]}"; do
  case "$t" in
    all)      want_kafka=1; want_backend=1; want_frontend=1 ;;
    apps)     want_backend=1; want_frontend=1 ;;
    kafka)    want_kafka=1 ;;
    backend)  want_backend=1 ;;
    frontend) want_frontend=1 ;;
    -h|--help|help) usage; exit 0 ;;
    *) echo "error: unknown target '$t'" >&2; usage; exit 1 ;;
  esac
done

LOG_DIR=logs
mkdir -p "$LOG_DIR"
pids=()

wait_for_http() { # <name> <url>
  local name=$1 url=$2 i
  printf 'Waiting for %s ' "$name"
  for i in $(seq 1 90); do
    if curl -fs -o /dev/null "$url"; then echo " ready."; return 0; fi
    printf '.'; sleep 2
  done
  echo " timed out (continuing anyway)."
}

run_kafka() {
  local name="${KAFKA_CONTAINER:-petclinic-kafka}"
  local image="${KAFKA_IMAGE:-docker.io/apache/kafka:3.9.0}"
  if podman container exists "$name"; then
    podman start "$name"
  else
    # Single-node KRaft mode (no Zookeeper).
    # Use dual listeners so both contexts work:
    # - PLAINTEXT (9092): container network clients (e.g., collector -> petclinic-kafka)
    # - PLAINTEXT_HOST (29092): host JVM clients (frontend/backend -> localhost)
    # apache/kafka uses KAFKA_* (no _CFG_ prefix) unlike the old bitnami image.
    podman run -d --name "$name" \
      --network petclinic-net \
      -p 9092:9092 \
      -p 29092:29092 \
      -e KAFKA_NODE_ID=0 \
      -e KAFKA_PROCESS_ROLES=controller,broker \
      -e KAFKA_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://:29092,CONTROLLER://:9093 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://petclinic-kafka:9092,PLAINTEXT_HOST://localhost:29092 \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
      -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      "$image"
  fi
  echo "Kafka broker '$name' starting (host bootstrap localhost:29092, container bootstrap petclinic-kafka:9092)."
  printf 'Waiting for Kafka (29092) '
  for i in $(seq 1 60); do
    if nc -z localhost 29092 2>/dev/null; then echo " open."; break; fi
    printf '.'; sleep 2
  done
}

# start_app_bg <name> <pom-dir> <health-url>
start_app_bg() {
  local name=$1 dir=$2 url=$3
  echo "Starting $name (log: $LOG_DIR/$name.log)..."
  # fork=false keeps the app in this Maven process so Ctrl+C stops it cleanly.
  MAVEN_OPTS="${MAVEN_OPTS:-}" \
    $MVN -q -f "$dir/pom.xml" -Dspring-boot.run.fork=false spring-boot:run \
    > "$LOG_DIR/$name.log" 2>&1 &
  pids+=($!)
  wait_for_http "$name" "$url"
}

# run_app_fg <name> <pom-dir>  (replaces this process; Ctrl+C stops the app)
run_app_fg() {
  local name=$1 dir=$2
  echo "Starting $name in the foreground (Ctrl+C to stop)..."
  export MAVEN_OPTS="${MAVEN_OPTS:-}"
  exec $MVN -f "$dir/pom.xml" -Dspring-boot.run.fork=false spring-boot:run
}

# ---- act, in canonical order: kafka, backend, frontend --------------------
[ $want_kafka -eq 1 ] && run_kafka

java_count=$((want_backend + want_frontend))

if [ "$java_count" -eq 0 ]; then
  [ $want_kafka -eq 1 ] && \
    echo "Kafka bootstrap   : localhost:29092"
  exit 0
fi

if [ "$java_count" -eq 1 ]; then
  if [ $want_backend -eq 1 ]; then
    run_app_fg backend backend
  else
    run_app_fg frontend frontend
  fi
fi

cleanup() {
  [ ${#pids[@]} -gt 0 ] && kill "${pids[@]}" 2>/dev/null
  wait 2>/dev/null
}

# Two or more apps: background them and wait together.
trap cleanup INT TERM
[ $want_backend -eq 1 ]  && start_app_bg backend  backend  "http://localhost:8081/actuator/health"
[ $want_frontend -eq 1 ] && start_app_bg frontend frontend "http://localhost:8080/actuator/health"

echo
echo "Services are up:"
[ $want_frontend -eq 1 ] && echo "  PetClinic UI      : http://localhost:8080/"
[ $want_backend -eq 1 ]  && echo "  Backend health    : http://localhost:8081/actuator/health"
[ $want_kafka -eq 1 ]    && echo "  Kafka bootstrap   : localhost:29092"
echo
echo "Logs in $LOG_DIR/. Press Ctrl+C to stop the app(s)."
wait
