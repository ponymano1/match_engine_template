#!/usr/bin/env bash
set -euo pipefail

NAME="${REDIS_CONTAINER:-match-engine-redis}"
PORT="${REDIS_PORT:-6379}"
IMAGE="${REDIS_IMAGE:-redis:7-alpine}"

if docker ps -a --format '{{.Names}}' | grep -qx "${NAME}"; then
  docker start "${NAME}" >/dev/null
else
  docker run -d --name "${NAME}" -p "${PORT}:6379" "${IMAGE}" >/dev/null
fi

for _ in $(seq 1 40); do
  if docker exec "${NAME}" redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "Redis ready on 127.0.0.1:${PORT} (container=${NAME})"
    exit 0
  fi
  sleep 0.25
done

echo "Redis failed to become ready" >&2
exit 1