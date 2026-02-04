# Rusty-LSM

A persistent, high-performance Key-Value storage engine implemented in Rust, utilizing a Log-Structured Merge-tree (LSM-Tree) architecture. This implementation focuses on minimizing memory overhead through zero-copy primitives and maximizing write throughput via lock-free data structures.



## Architectural Design

### Memory Management and Serialization
The engine minimizes CPU cycles and memory allocations by utilizing `rkyv` for zero-copy serialization and `memmap2` for file I/O. 

* **SSTable Retrieval:** SSTable files are memory-mapped into the process address space. When a query hits the disk, the engine accesses the raw bytes directly.
* **Zero-Copy Parsing:** By using `rkyv`, the engine can cast pointers from the memory-mapped region directly to Rust structs. This eliminates the traditional "Read -> Buffer -> Deserialize -> Heap Allocate" pipeline, allowing for O(log N) lookups with zero allocation overhead.

### Concurrency and Write Path
The write path is designed for high concurrency to prevent thread contention under heavy write loads.

* **MemTable:** Implemented as a lock-free SkipList via `crossbeam-skiplist`. This allows multiple writers to insert data concurrently and multiple readers to perform point-lookups without acquiring a global `RwLock` or `Mutex`.
* **Write-Ahead Log (WAL):** To ensure durability, every operation is serialized to a WAL before being committed to the MemTable. The WAL uses `tokio::io::BufWriter` and explicit `sync_all` calls to guarantee that data is committed to the physical storage medium before the operation returns.



### Read Path Optimization
The engine implements a multi-tiered search strategy:
1.  **MemTable Search:** A lock-free search is performed on the active in-memory SkipList.
2.  **SSTable Search:** If the key is not in memory, the engine iterates through immutable SSTables in reverse chronological order. Binary search is performed on the memory-mapped archived vectors.

## Data Durability and Recovery

### Crash Consistency
The engine maintains atomicity between the WAL and the MemTable. On initialization, the engine performs a recovery scan of the WAL:
* The log is read sequentially.
* CRC32 checksums are verified for every entry to detect partial writes or hardware-level corruption.
* Valid entries are re-inserted into the MemTable, restoring the engine to its state prior to shutdown or failure.



### Persistence Lifecycle
When the MemTable reaches its capacity threshold, it undergoes a rotation. A new MemTable and WAL are initialized, while the previous MemTable is flushed to disk as an immutable SSTable. This process ensures that memory usage remains bounded while maintaining an append-only write pattern on disk, which is optimized for both SSD and HDD sequential write speeds.

## Technical Specifications

| Component | Implementation Detail |
| :--- | :--- |
| **Concurrency** | Lock-free SkipList (Crossbeam) |
| **I/O Model** | Async/Await with Tokio Runtime |
| **Persistence** | Memory-mapped SSTables (rkyv) |
| **Integrity** | CRC32fast Checksumming |
| **Memory Buffer** | Reference-counted `bytes::Bytes` |

## Project Structure

* `src/engine.rs`: Orchestration of the write path, read path, and recovery logic.
* `src/memtable.rs`: Concurrent in-memory storage implementation.
* `src/wal.rs`: Append-only durability layer and replay logic.
* `src/sstable/builder.rs`: Logic for flushing MemTables to immutable disk files.
* `src/sstable/reader.rs`: Zero-copy retrieval logic using memory mapping.
* `src/error.rs`: Centralized error definitions for I/O and serialization failures.

## Usage

### Testing
The engine includes integration tests that simulate full system restarts to verify the recovery mechanism.
```bash
cargo test
