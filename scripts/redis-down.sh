#!/usr/bin/env bash
set -euo pipefail
NAME="${REDIS_CONTAINER:-match-engine-redis}"
docker rm -f "${NAME}" >/dev/null 2>&1 || true
echo "Removed Redis container ${NAME}"