#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

./scripts/redis-up.sh
trap './scripts/redis-down.sh' EXIT

export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/}"

cargo test --features redis-mq --test integration_redis -- --nocapture --test-threads=4 "$@"