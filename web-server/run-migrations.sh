#!/bin/bash
set -e

echo "⏳ Waiting for PostgreSQL to be ready..."
while ! pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
  sleep 1
done
echo "✅ PostgreSQL is ready."

echo "🚀 Running migrations from /migrations/*.up.sql"
for f in /migrations/*.up.sql; do
  echo "🔹 Running migration: $f"
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$f"
done
echo "✅ All migrations executed."
