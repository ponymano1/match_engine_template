#!/usr/bin/env bash
# 全量测试: 起 Redis → cargo test(含集成) → 退出时关 Redis
set -euo pipefail
cd "$(dirname "$0")/.."

./scripts/redis-up.sh
trap './scripts/redis-down.sh' EXIT

export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/}"

cargo test -- --include-ignored "$@"
