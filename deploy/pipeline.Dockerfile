# syntax=docker/dockerfile:1.7
#
# Nightly pipeline image (curate / validate / publish), linux/amd64 and linux/arm64.
# Build context is the repo root:
#   docker buildx build -f deploy/pipeline.Dockerfile --platform linux/amd64,linux/arm64 .
# Installs pipeline/ (console script `book-pipeline`) and ships schema/ at BOOK_SCHEMA_DIR.
# compose runs ${PIPELINE_COMMAND}, default "book-pipeline nightly".

FROM ghcr.io/astral-sh/uv:0.5.11 AS uv

FROM python:3.12-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_COMPILE_BYTECODE=1 \
    BOOK_SCHEMA_DIR=/app/schema \
    TZ=UTC

WORKDIR /app
COPY pipeline/ /app/pipeline/
COPY schema/ /app/schema/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system /app/pipeline

RUN groupadd --system --gid 10001 pipeline \
 && useradd --system --uid 10001 --gid pipeline --home-dir /work --shell /usr/sbin/nologin pipeline \
 && install -d -o pipeline -g pipeline /work
USER 10001:10001
WORKDIR /work
