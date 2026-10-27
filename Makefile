EXTENSION = pg_incremental

MODULE_big = $(EXTENSION)

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--1.0.sql
SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))
REGRESS = sequence time_interval file_list security upgrade

PG_CPPFLAGS = -Iinclude
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

# PostgreSQL does not allow declaration after statement, but we do
override CFLAGS := $(filter-out -Wdeclaration-after-statement,$(CFLAGS))

REGRESS_DBNAME ?= contrib_regression
PSQL = $(shell $(PG_CONFIG) --bindir)/psql

# Override installcheck to run tests directly with psql, bypassing pg_regress
# which requires fork+exec of /bin/sh (unavailable in some environments).
.PHONY: installcheck
installcheck: install
	@mkdir -p results
	@$(PSQL) -c "CREATE DATABASE $(REGRESS_DBNAME)" 2>/dev/null || true
	@failed=0; \
	for test in $(REGRESS); do \
	    printf "%-30s" "$$test ..."; \
	    $(PSQL) -X -q -a -d $(REGRESS_DBNAME) \
	        <sql/$$test.sql >results/$$test.out 2>&1; \
	    if diff -q expected/$$test.out results/$$test.out >/dev/null 2>&1; then \
	        echo "ok"; \
	    else \
	        echo "FAILED"; \
	        diff expected/$$test.out results/$$test.out \
	            >results/$$test.out.diff 2>&1 || true; \
	        failed=$$((failed + 1)); \
	    fi; \
	done; \
	if [ $$failed -eq 0 ]; then \
	    echo "All tests passed."; \
	else \
	    echo "$$failed test(s) failed. See files in results/."; \
	    exit 1; \
	fi
