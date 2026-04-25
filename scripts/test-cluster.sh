#!/usr/bin/env bash
# End-to-end integration test for a 3-node raft cluster.
# Usage: ./scripts/test-cluster.sh [--logs]
#
#   --logs   stream node logs to stderr in real-time
#
# Set RUST_LOG to control log verbosity (default: raft=info).
# Examples:
#   RUST_LOG=raft=debug ./scripts/test-cluster.sh --logs
#   RUST_LOG=raft=trace ./scripts/test-cluster.sh --logs
set -euo pipefail

SHOW_LOGS=0
for arg in "$@"; do
    case "$arg" in
        --logs) SHOW_LOGS=1 ;;
        *) echo "unknown argument: $arg" >&2; exit 1 ;;
    esac
done

# Find project root (directory containing Cargo.toml)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

BINARY="./target/release/raft"
PASS=0
FAIL=0

ok()   { echo "  PASS  $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL  $1  (expected '$2', got '$3')"; FAIL=$((FAIL + 1)); }

check() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$actual" = "$expected" ]; then ok "$desc"; else fail "$desc" "$expected" "$actual"; fi
}

check_status() {
    local desc="$1" expected="$2"
    local actual
    actual=$(curl -s -o /dev/null -w "%{http_code}" -m 3 "${@:3}")
    if [ "$actual" = "$expected" ]; then ok "$desc"; else fail "$desc" "$expected" "$actual"; fi
}

echo "Building..."
cargo build --release --quiet
echo ""

DATA=$(mktemp -d)
PIDS=()
TAIL_PID=""

cleanup() {
    for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
    [ -n "$TAIL_PID" ] && kill "$TAIL_PID" 2>/dev/null || true
    rm -rf "$DATA"
}
trap cleanup EXIT

echo "Starting 3-node cluster (data: $DATA)..."

# Pre-create log files so tail -f can open them before the processes write anything.
touch "$DATA/node1.log" "$DATA/node2.log" "$DATA/node3.log"

if [ "$SHOW_LOGS" = "1" ]; then
    tail -f "$DATA/node1.log" "$DATA/node2.log" "$DATA/node3.log" >&2 &
    TAIL_PID=$!
fi

"$BINARY" --id 1 --addr 127.0.0.1:9001 \
    --peer 2=127.0.0.1:9002 --peer 3=127.0.0.1:9003 \
    --data-dir "$DATA/node1" --client-addr 127.0.0.1:8001 \
    2>"$DATA/node1.log" &
PIDS+=($!)

"$BINARY" --id 2 --addr 127.0.0.1:9002 \
    --peer 1=127.0.0.1:9001 --peer 3=127.0.0.1:9003 \
    --data-dir "$DATA/node2" --client-addr 127.0.0.1:8002 \
    2>"$DATA/node2.log" &
PIDS+=($!)

"$BINARY" --id 3 --addr 127.0.0.1:9003 \
    --peer 1=127.0.0.1:9001 --peer 2=127.0.0.1:9002 \
    --data-dir "$DATA/node3" --client-addr 127.0.0.1:8003 \
    2>"$DATA/node3.log" &
PIDS+=($!)

echo "Waiting for leader election..."
LEADER=""
for _ in $(seq 1 30); do
    for port in 8001 8002 8003; do
        result=$(curl -s -m 1 -X PUT "http://127.0.0.1:$port/kv/__probe" -d "x" 2>/dev/null || true)
        if [ "$result" = "ok" ]; then
            LEADER=$port
            curl -s -m 1 -X DELETE "http://127.0.0.1:$port/kv/__probe" >/dev/null 2>&1 || true
            break 2
        fi
    done
    sleep 0.2
done

if [ -z "$LEADER" ]; then
    echo "ERROR: no leader elected after 6 s"
    echo "--- node logs ---"
    cat "$DATA"/node*.log
    exit 1
fi

# Find a follower port
FOLLOWER=""
for port in 8001 8002 8003; do
    [ "$port" != "$LEADER" ] && FOLLOWER=$port && break
done

echo "Leader: port $LEADER  |  Follower: port $FOLLOWER"
echo ""

echo "Running tests..."

check "PUT new key" "ok" \
    "$(curl -s -m 3 -X PUT "http://127.0.0.1:$LEADER/kv/db-primary" -d "10.0.0.1:5432")"

check "GET existing key" "10.0.0.1:5432" \
    "$(curl -s -m 3 "http://127.0.0.1:$LEADER/kv/db-primary")"

check "PUT update key" "ok" \
    "$(curl -s -m 3 -X PUT "http://127.0.0.1:$LEADER/kv/db-primary" -d "10.0.0.2:5432")"

check "GET updated key" "10.0.0.2:5432" \
    "$(curl -s -m 3 "http://127.0.0.1:$LEADER/kv/db-primary")"

check "PUT second key" "ok" \
    "$(curl -s -m 3 -X PUT "http://127.0.0.1:$LEADER/kv/region" -d "eu-west")"

check "GET second key" "eu-west" \
    "$(curl -s -m 3 "http://127.0.0.1:$LEADER/kv/region")"

check "DELETE key" "ok" \
    "$(curl -s -m 3 -X DELETE "http://127.0.0.1:$LEADER/kv/db-primary")"

check_status "GET deleted key returns 404" "404" \
    "http://127.0.0.1:$LEADER/kv/db-primary"

check_status "PUT to non-leader returns 503" "503" \
    -X PUT "http://127.0.0.1:$FOLLOWER/kv/x" -d "y"

check_status "GET to non-leader returns 503" "503" \
    "http://127.0.0.1:$FOLLOWER/kv/region"

check_status "GET missing key returns 404" "404" \
    "http://127.0.0.1:$LEADER/kv/does-not-exist"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
