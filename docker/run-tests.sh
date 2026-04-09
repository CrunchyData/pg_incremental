#!/usr/bin/env bash
# Build the test image(s), start PostgreSQL with pg_cron, then build and installcheck from a copy of
# the repo under /tmp so the bind-mounted working tree is never modified (compose mounts ..:/work:ro).
#
# Invoke from repo root: ./docker/run-tests.sh
#
# Environment:
#   PG_VERSIONS  — space-separated majors to test (default: "17 18").
#
# PostgreSQL in the container needs shared_preload_libraries and cron.database_name for
# contrib_regression (see docker/init-pg-cron.sh).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PG_VERSIONS="${PG_VERSIONS:-17 18}"
COMPOSE=(docker compose -f docker/docker-compose.yml)

run_one() {
	local major="$1"
	local svc="postgres-${major}"

	"${COMPOSE[@]}" build "$svc"
	"${COMPOSE[@]}" down -v --remove-orphans 2>/dev/null || true
	"${COMPOSE[@]}" up -d "$svc"

	if ! "${COMPOSE[@]}" exec -T "$svc" bash -lc \
		'for i in $(seq 1 90); do pg_isready -U postgres && exit 0; sleep 1; done; exit 1'; then
		echo "PostgreSQL $major did not become ready; logs:" >&2
		"${COMPOSE[@]}" logs "$svc" >&2 || true
		"${COMPOSE[@]}" down -v --remove-orphans 2>/dev/null || true
		return 1
	fi

	"${COMPOSE[@]}" exec -u root -T "$svc" bash -lc '
set -euo pipefail
BUILD=/tmp/pg_incremental_build
rm -rf "$BUILD"
cp -a /work "$BUILD"
cd "$BUILD"
make clean && make && make install
chown -R postgres:postgres "$BUILD"
su postgres -s /bin/bash -c "cd $BUILD && make -o install installcheck"
rm -rf "$BUILD"
'

	"${COMPOSE[@]}" down -v --remove-orphans
	echo "installcheck: OK (PostgreSQL $major)"
}

failed=0
for major in $PG_VERSIONS; do
	echo "========== PostgreSQL $major =========="
	if ! run_one "$major"; then
		failed=1
		break
	fi
done

if [[ "$failed" -ne 0 ]]; then
	exit 1
fi
echo "All requested versions passed installcheck: $PG_VERSIONS"
