FROM rust:latest AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations

RUN cargo build --release

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y ca-certificates libssl3t64 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/automated_trading /usr/local/bin/automated_trading

EXPOSE 4500

CMD ["automated_trading"]
