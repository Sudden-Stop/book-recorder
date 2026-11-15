# syntax=docker/dockerfile:1.7
#
# book-recorder image, linux/amd64 and linux/arm64.
# Build context is the repo root:
#   docker buildx build -f deploy/Dockerfile --platform linux/amd64,linux/arm64 .
#
# The Rust build runs on the builder's native platform and cross-compiles to the target
# (no QEMU for the slow part); only the small runtime stage runs under emulation.

ARG RUST_VERSION=1.88
ARG DEBIAN_RELEASE=bookworm

FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS build
ARG TARGETARCH
ARG BUILDARCH

# Map the Docker arch to a Rust target and a GNU toolchain prefix; install the cross
# compiler only when target and builder differ (C code in ring and zstd-sys needs it).
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64-unknown-linux-gnu;  gnu=x86_64-linux-gnu ;; \
      arm64) triple=aarch64-unknown-linux-gnu; gnu=aarch64-linux-gnu ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac; \
    if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      apt-get update; \
      apt-get install -y --no-install-recommends "gcc-$(echo "$gnu" | tr _ -)" "libc6-dev-$TARGETARCH-cross"; \
      rm -rf /var/lib/apt/lists/*; \
    fi; \
    rustup target add "$triple"; \
    echo "$triple" > /rust-target; \
    echo "$gnu" > /gnu-prefix

WORKDIR /src
COPY recorder/ ./

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,id=book-recorder-target-${TARGETARCH},sharing=locked \
    set -eux; \
    triple="$(cat /rust-target)"; gnu="$(cat /gnu-prefix)"; \
    envtriple="$(echo "$triple" | tr 'a-z-' 'A-Z_')"; \
    export "CARGO_TARGET_${envtriple}_LINKER=${gnu}-gcc"; \
    export "CC_$(echo "$triple" | tr - _)=${gnu}-gcc"; \
    export "AR_$(echo "$triple" | tr - _)=${gnu}-ar"; \
    locked=""; [ -f Cargo.lock ] && locked="--locked"; \
    cargo build --release $locked --target "$triple" --bin book-recorder; \
    install -D -m 0755 "target/$triple/release/book-recorder" /out/book-recorder

FROM debian:${DEBIAN_RELEASE}-slim AS runtime

# ca-certificates for any native-roots TLS, curl for the compose healthcheck.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl tzdata; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd --system --gid 10001 recorder; \
    useradd --system --uid 10001 --gid recorder --home-dir /data --shell /usr/sbin/nologin recorder; \
    install -d -o recorder -g recorder /data

COPY --from=build /out/book-recorder /usr/local/bin/book-recorder

USER 10001:10001
ENV BR_DATA_DIR=/data \
    BR_HEALTH_PORT=8080 \
    TZ=UTC
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=10s --start-period=2m --retries=3 \
    CMD curl -fsS -m 5 http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/book-recorder"]
