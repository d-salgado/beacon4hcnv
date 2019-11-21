#!/usr/bin/env bash

pg_restore --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" --verbose "/tmp/dump.backup" 