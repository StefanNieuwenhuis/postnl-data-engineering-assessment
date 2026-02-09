#!/bin/sh
# MinIO init: create buckets and seed landing zone. Run inside minio/mc container.
set -e

MINIO_ENDPOINT="http://${MINIO_HOST:-minio}:${MINIO_PORT:-9000}"

echo "Configuring mc alias..."
mc alias set myminio "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER:-minio}" "${MINIO_ROOT_PASSWORD:-minio123}"

echo "Creating buckets..."
mc mb myminio/landing --ignore-existing
mc mb myminio/bronze --ignore-existing
mc mb myminio/silver --ignore-existing
mc mb myminio/gold --ignore-existing

echo "Seeding landing zone..."
mc cp /seed/shipments.csv myminio/landing/sources/shipments.csv 2>/dev/null || true
mc cp /seed/vehicles.csv myminio/landing/sources/vehicles.csv 2>/dev/null || true
mc cp /seed/routes.json myminio/landing/sources/routes/routes.json 2>/dev/null || true
mc cp /seed/weather.json myminio/landing/sources/weather/weather.json 2>/dev/null || true

echo "Buckets created (landing seeded if data/raw exists)."