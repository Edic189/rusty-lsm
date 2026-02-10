FROM rust:1.84 AS builder

WORKDIR /usr/src/rusty-lsm

RUN apt-get update && apt-get install -y libssl-dev pkg-config

COPY Cargo.toml Cargo.lock ./

RUN mkdir src

RUN echo "fn main() {}" > src/main.rs
RUN echo "" > src/lib.rs

RUN cargo build --release

RUN rm -rf src

COPY . .

RUN touch src/main.rs src/lib.rs

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/rusty-lsm/target/release/rusty-lsm .

EXPOSE 8080

RUN mkdir -p db_data

ENTRYPOINT ["./rusty-lsm", "start", "--port", "8080", "--data-dir", "./db_data"]
