#!/bin/sh
# Run from the project directory on Linux/WSL/Git Bash.
set -eu
docker compose ps
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic orders
docker compose run --rm mc ls local/
docker compose exec -T jupyter python3 scripts/partie1_cluster.py
