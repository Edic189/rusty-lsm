# Faza 1: Builder
# Koristimo veliko AS da maknemo warning
FROM rust:1.75 AS builder

WORKDIR /usr/src/rusty-lsm

# Instaliraj potrebne sistemske alate za kompajliranje (za svaki slučaj)
RUN apt-get update && apt-get install -y libssl-dev pkg-config

# --- TRIK ZA KEŠIRANJE (Popravljen za lib+bin) ---
# 1. Kopiraj definicije
COPY Cargo.toml Cargo.lock ./

# 2. Kreiraj prazan src folder
RUN mkdir src

# 3. Kreiraj dummy main.rs I dummy lib.rs
# Ovo je ključno! Cargo mora vidjeti oba fajla da bi povukao dependencyje
RUN echo "fn main() {}" > src/main.rs
RUN echo "" > src/lib.rs

# 4. Buildaj samo dependencyje
RUN cargo build --release

# 5. Obriši lažne fajlove da ne smetaju
RUN rm -rf src

# --- PRAVI BUILD ---
# 6. Kopiraj pravi izvorni kod
COPY . .

# 7. Trik: Ažuriraj timestamp fajlova da Cargo shvati da su novi
RUN touch src/main.rs src/lib.rs

# 8. Pravi build tvoje aplikacije
RUN cargo build --release

# Faza 2: Runtime (Mali image)
FROM debian:bookworm-slim

# Instaliraj OpenSSL (često potrebno za mrežne stvari)
RUN apt-get update && apt-get install -y libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Kopiraj izvršnu datoteku iz builder faze
COPY --from=builder /usr/src/rusty-lsm/target/release/rusty-lsm .

# Otvori port
EXPOSE 8080

# Kreiraj folder za podatke
RUN mkdir -p db_data

# Pokreni
ENTRYPOINT ["./rusty-lsm", "start", "--port", "8080", "--data-dir", "./db_data"]
