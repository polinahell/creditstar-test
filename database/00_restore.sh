#!/bin/bash
# Restores the de_test_task_db PostgreSQL custom-format dump into $POSTGRES_DB.
# This script is sourced by the PostgreSQL Docker entrypoint from
# /docker-entrypoint-initdb.d/, so POSTGRES_USER and POSTGRES_DB are available.
set -e

echo "[restore] Starting pg_restore for $POSTGRES_DB ..."
pg_restore \
    --no-owner \
    --no-privileges \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    /docker-entrypoint-initdb.d/de_test_task_db
echo "[restore] Done."
