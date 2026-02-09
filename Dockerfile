# Faza 1: Builder
# Koristimo noviju verziju Rusta koja podržava lock file v4
FROM rust:1.84 AS builder

WORKDIR /usr/src/rusty-lsm

RUN apt-get update && apt-get install -y libssl-dev pkg-config

# --- TRIK ZA KEŠIRANJE ---
COPY Cargo.toml Cargo.lock ./

RUN mkdir src

# Kreiraj lažne ulazne točke
RUN echo "fn main() {}" > src/main.rs
RUN echo "" > src/lib.rs

# Buildaj dependencyje
RUN cargo build --release

# Obriši lažne fajlove
RUN rm -rf src

# --- PRAVI BUILD ---
COPY . .

# Ažuriraj timestamp
RUN touch src/main.rs src/lib.rs

# Pravi build
RUN cargo build --release

# Faza 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/rusty-lsm/target/release/rusty-lsm .

EXPOSE 8080

RUN mkdir -p db_data

ENTRYPOINT ["./rusty-lsm", "start", "--port", "8080", "--data-dir", "./db_data"]
