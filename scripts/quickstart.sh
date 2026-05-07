#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f ./certs/server.crt ] || [ ! -f ./certs/server.key ]; then
  echo "Generating self-signed certs in ./certs"
  mkdir -p ./certs
  ./scripts/gen_cert.sh ./certs/server.crt ./certs/server.key
fi

export DATA_DIR="$(pwd)/data"
export TLS_CERT="$(pwd)/certs/server.crt"
export TLS_KEY="$(pwd)/certs/server.key"
export JWT_SECRET="secret123"
export ADMIN_PASSWORD="admin123"

docker-compose up --build -d

echo "Waiting for container to become healthy..."
sleep 5

docker-compose ps

echo "Open https://localhost:8443 in your browser."
