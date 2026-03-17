FROM rust:latest AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Step 1: Cache dependencies — only re-runs when Cargo.toml/Cargo.lock change
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# Step 2: Build actual source — fast incremental rebuild
COPY src ./src
COPY migrations ./migrations
RUN touch src/main.rs && cargo build --release

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y ca-certificates libssl3t64 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/automated_trading /usr/local/bin/automated_trading

EXPOSE 4500

CMD ["automated_trading"]
