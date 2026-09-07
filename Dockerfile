# Multi-stage build for the KV store node (kvstored) and CLI (kvctl).

FROM rust:1.85-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler clang libclang-dev cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --release --bin kvstored --bin kvctl

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/kvstored /usr/local/bin/kvstored
COPY --from=builder /app/target/release/kvctl /usr/local/bin/kvctl
ENV KV_DATA_DIR=/data
EXPOSE 5400
ENTRYPOINT ["kvstored"]
