# Rusty LSM

Rusty LSM is a high-performance, persistent key-value store based on the Log-Structured Merge-tree (LSM) architecture. It is written in Rust and operates as a standalone server implementing the Redis Serialization Protocol (RESP), making it compatible with existing Redis clients and tools.

The system is designed for high write throughput and low-latency reads, utilizing asynchronous I/O, memory mapping, and zero-copy deserialization.

## Technical Architecture

### Core Components
* **MemTable:** Utilizes `crossbeam-skiplist` for lock-free concurrent writes and reads in memory.
* **WAL (Write-Ahead Log):** Ensures durability. All writes are appended to the log with CRC32 checksums before being applied to the MemTable. Supports automatic recovery and truncation of corrupted tail records.
* **SSTable (Sorted String Table):** Immutable disk files structured in 4KB blocks.
    * **Compression:** Google Snappy algorithm via `snap`.
    * **Serialization:** Zero-copy deserialization using `rkyv`.
    * **Indexing:** Bloom Filters for probabilistically rejecting non-existent keys and Sparse Index for locating data blocks.
* **Manifest:** Atomic state management for tracking active file levels and generation IDs.

### Network Protocol
The server implements a subset of the RESP (Redis Serialization Protocol), allowing interaction via standard Redis clients.

**Supported Commands:**
* `SET key value` - Write data.
* `GET key` - Read data.
* `DEL key` - Delete data.
* `EXISTS key` - Check if a key exists.
* `PING` - Health check.
* `FLUSHDB` - Triggers a manual flush of MemTable to disk.
* `STATS` - Returns internal engine metrics (LSM levels, cache usage).

## Performance

Benchmarks were conducted using `redis-benchmark` on standard hardware via Docker containerization.

* **Write Throughput (SET):** ~20,000 requests/sec
* **Read Throughput (GET):** ~41,000 requests/sec
* **P50 Latency:** < 3ms

## Build and Run

### Requirements
* Rust 1.85 (Edition 2024)
* Docker (Optional)

### Running via Docker
The project includes a multi-stage Dockerfile for optimized builds.

1.  **Build the image:**
    ```bash
    docker build -t rusty-lsm .
    ```

2.  **Run the container:**
    ```bash
    # Runs on port 8080 with increased file descriptor limits for LSM handling
    docker run -d -p 8080:8080 --ulimit nofile=65535:65535 --name rusty-lsm rusty-lsm
    ```

### Running Locally (Cargo)

```bash
cargo run --release -- start --port 8080 --data-dir ./db_data
```

**CLI Arguments:**
* `--port`: TCP port to listen on (default: 8080).
* `--data-dir`: Directory for storing WAL and SSTables (default: ./db_data).
* `--memtable-size`: Threshold in bytes for flushing to disk (default: 64MB).
* `--block-size`: SSTable block size (default: 4KB).

## Usage Example

Since Rusty LSM speaks RESP, you can use `redis-cli`:

```bash
$ redis-cli -p 8080

127.0.0.1:8080> PING
PONG
127.0.0.1:8080> SET user:100 "Rustacean"
OK
127.0.0.1:8080> GET user:100
"Rustacean"
127.0.0.1:8080> STATS
"--- DB Stats ---
 MemTable Size: 0.12 MB
 SSTables (Active): 4
 Level Distribution: [2, 1, 0, 0, 0, 0, 0]
 ----------------"
```

## Configuration & Tuning

The engine's performance characteristics can be tuned via `LsmConfig`:

* **block_cache_capacity:** Number of blocks to keep in the LRU cache.
* **memtable_capacity:** Controls the frequency of flushes. Larger tables improve write performance but increase recovery time.
* **compaction_thresholds:** Configurable triggers for L0 and Ln compaction processes.

## License

This project is licensed under the MIT License.
