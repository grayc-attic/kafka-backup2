#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE "kafka_backup";
    CREATE USER "kafka_backup" WITH ENCRYPTED PASSWORD 'changeMe';
    GRANT ALL PRIVILEGES ON DATABASE "kafka_backup" TO "kafka_backup"";
EOSQL
