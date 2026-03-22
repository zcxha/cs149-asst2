#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

MODE="${1:-async}"
THREADS="${2:-8}"
TIMEOUT_SECS="${TIMEOUT_SECS:-20}"

SYNC_TESTS=(
  simple_test_sync
  ping_pong_equal
  ping_pong_unequal
  super_light
  super_super_light
  recursive_fibonacci
  math_operations_in_tight_for_loop
  math_operations_in_tight_for_loop_fewer_tasks
  math_operations_in_tight_for_loop_fan_in
  math_operations_in_tight_for_loop_reduction_tree
  spin_between_run_calls
  mandelbrot_chunked
)

ASYNC_TESTS=(
  simple_test_async
  ping_pong_equal_async
  ping_pong_unequal_async
  super_light_async
  super_super_light_async
  recursive_fibonacci_async
  math_operations_in_tight_for_loop_async
  math_operations_in_tight_for_loop_fewer_tasks_async
  math_operations_in_tight_for_loop_fan_in_async
  math_operations_in_tight_for_loop_reduction_tree_async
  mandelbrot_chunked_async
  spin_between_run_calls_async
  simple_run_deps_test
  strict_diamond_deps_async
  strict_graph_deps_small_async
  strict_graph_deps_med_async
  strict_graph_deps_large_async
)

usage() {
  cat <<EOF
Usage: ./test_correctness.sh [async|sync|all] [num_threads]

Examples:
  ./test_correctness.sh
  ./test_correctness.sh async 8
  ./test_correctness.sh all 4

Environment:
  TIMEOUT_SECS   Per-test timeout in seconds (default: 20)
EOF
}

case "$MODE" in
  async)
    TESTS=("${ASYNC_TESTS[@]}")
    ;;
  sync)
    TESTS=("${SYNC_TESTS[@]}")
    ;;
  all)
    TESTS=("${SYNC_TESTS[@]}" "${ASYNC_TESTS[@]}")
    ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    usage >&2
    exit 2
    ;;
esac

echo "Building part_b..."
make >/dev/null

FAILURES=0
TOTAL=0

run_one() {
  local test_name="$1"
  local log_file
  log_file="$(mktemp)"

  TOTAL=$((TOTAL + 1))
  echo
  echo "==> [$TOTAL/${#TESTS[@]}] $test_name"

  if timeout "${TIMEOUT_SECS}s" ./runtasks -n "$THREADS" -i 1 "$test_name" >"$log_file" 2>&1; then
    echo "PASS: $test_name"
  else
    FAILURES=$((FAILURES + 1))
    echo "FAIL: $test_name"
    cat "$log_file"
  fi

  rm -f "$log_file"
}

for test_name in "${TESTS[@]}"; do
  run_one "$test_name"
done

echo
echo "Finished: $((TOTAL - FAILURES)) passed, $FAILURES failed."

if [[ "$FAILURES" -ne 0 ]]; then
  exit 1
fi
