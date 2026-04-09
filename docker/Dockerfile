# PostgreSQL + pg_cron for pg_incremental regression tests.
# Build from repository root: docker build -f docker/Dockerfile .
#
# Args:
#   PG_MAJOR        — server major (e.g. 17, 18); must match base image
#   PG_IMAGE_SUFFIX — tag after major (e.g. bookworm, trixie) for postgres:${PG_MAJOR}-${PG_IMAGE_SUFFIX}

ARG PG_MAJOR=17
ARG PG_IMAGE_SUFFIX=bookworm
FROM postgres:${PG_MAJOR}-${PG_IMAGE_SUFFIX}

ARG PG_MAJOR
# v1.6.6+ required for PostgreSQL 18 (PortalRun signature); keep in sync across all PG majors in compose.
ARG PG_CRON_REF=v1.6.7

USER root
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		build-essential \
		git \
		ca-certificates \
		postgresql-server-dev-${PG_MAJOR} \
	&& rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch "${PG_CRON_REF}" https://github.com/citusdata/pg_cron.git /tmp/pg_cron \
	&& make -C /tmp/pg_cron PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config \
	&& make -C /tmp/pg_cron PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config install \
	&& rm -rf /tmp/pg_cron

COPY docker/init-pg-cron.sh /docker-entrypoint-initdb.d/10-pg-cron.sh
RUN chmod +x /docker-entrypoint-initdb.d/10-pg-cron.sh

USER postgres
