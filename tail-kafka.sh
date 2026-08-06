#!/usr/bin/env bash
#
# Tail logs from the Kafka broker container.
#
# Usage:
#   ./tail-kafka.sh [follow|tail|since] [arg]
#
# Commands:
#   follow          Follow logs continuously (default)
#   tail [N]        Show last N lines (default: 200)
#   since [DUR]     Show logs since a duration (default: 10m), e.g. 30s, 5m, 1h
#
# Examples:
#   ./tail-kafka.sh
#   ./tail-kafka.sh follow
#   ./tail-kafka.sh tail 500
#   ./tail-kafka.sh since 15m
set -euo pipefail
cd "$(dirname "$0")"

KAFKA_CONTAINER="${KAFKA_CONTAINER:-petclinic-kafka}"

usage() {
  cat <<'EOF'
Usage: ./tail-kafka.sh [follow|tail|since] [arg]

Commands:
  follow          Follow logs continuously (default)
  tail [N]        Show last N lines (default: 200)
  since [DUR]     Show logs since a duration (default: 10m)

Examples:
  ./tail-kafka.sh
  ./tail-kafka.sh follow
  ./tail-kafka.sh tail 500
  ./tail-kafka.sh since 15m
EOF
}

ensure_container_exists() {
  if ! podman container exists "$KAFKA_CONTAINER" 2>/dev/null; then
    echo "error: Kafka container '$KAFKA_CONTAINER' does not exist." >&2
    echo "Start it with: ./run-all.sh kafka" >&2
    exit 1
  fi
}

cmd="${1:-follow}"
arg="${2:-}"

case "$cmd" in
  follow)
    ensure_container_exists
    echo "Following Kafka logs from '$KAFKA_CONTAINER'..."
    exec podman logs -f "$KAFKA_CONTAINER"
    ;;
  tail)
    ensure_container_exists
    lines="${arg:-200}"
    [[ "$lines" =~ ^[0-9]+$ ]] || { echo "error: tail count must be a number" >&2; exit 1; }
    exec podman logs --tail "$lines" "$KAFKA_CONTAINER"
    ;;
  since)
    ensure_container_exists
    since="${arg:-10m}"
    exec podman logs --since "$since" "$KAFKA_CONTAINER"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "error: unknown command '$cmd'" >&2
    usage
    exit 1
    ;;
esac
