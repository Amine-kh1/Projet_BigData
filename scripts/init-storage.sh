#!/bin/sh
set -eu

# The Compose healthcheck handles normal startup; retry also covers a restart.
attempt=0
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 30 ] || { echo "MinIO remains unavailable" >&2; exit 1; }
  sleep 2
done
mc mb --ignore-existing local/lake
mc ls local/
