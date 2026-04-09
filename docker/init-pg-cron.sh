#!/bin/bash
set -euo pipefail
# Runs during init after initdb; the long-running server reads this before first start.
# contrib_regression is created later by pg_regress; cron.database_name must match it
# so CREATE EXTENSION pg_cron can run there (--load-extension in Makefile).
cat >>"${PGDATA}/postgresql.conf" <<'EOF'

# pg_cron (docker / regression)
shared_preload_libraries = 'pg_cron'
cron.database_name = 'contrib_regression'
EOF
