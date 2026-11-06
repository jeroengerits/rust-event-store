FROM rust:1-bookworm AS build
WORKDIR /build
COPY . .
RUN cargo build --release --locked --bin eventstore

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 eventstore \
    && useradd --system --uid 10001 --gid eventstore --no-create-home eventstore
COPY --from=build /build/target/release/eventstore /usr/local/bin/eventstore
USER 10001:10001
WORKDIR /
ENV EVENTSTORE_HOST=0.0.0.0 \
    EVENTSTORE_PORT=8080
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl --fail --silent --show-error "http://127.0.0.1:${EVENTSTORE_PORT}/health" || exit 1
ENTRYPOINT ["eventstore"]
CMD ["serve"]
