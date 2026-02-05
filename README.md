# Rusty LSM: Persistent Key-Value Storage Engine

Rusty LSM is a high-performance, persistent key-value storage engine implemented in Rust. It utilizes a Log-Structured Merge-Tree (LSM-Tree) architecture, designed for high write throughput and efficient querying. The engine supports atomic persistence, crash recovery, and tiered storage management.

## Key Features

* **LSM-Tree Architecture:** Optimized for sequential write performance and minimized disk I/O.
* **Crash Recovery:** Implements a Write-Ahead Log (WAL) to ensure data durability and system state restoration after a failure.
* **In-Memory Storage:** Uses a concurrent, lock-free SkipList (via `crossbeam-skiplist`) for high-performance memory operations.
* **Block-Based SSTables:** Disk storage is organized into 4KB blocks with sparse indexing, minimizing memory overhead and optimizing block-level access.
* **Leveled Compaction:** Advanced background compaction strategy (L0 -> L1 -> L2...) to manage write amplification and optimize read performance.
* **Manifest Management:** Atomic state tracking via `manifest.json` ensures database consistency across restarts and compaction cycles.
* **Zero-Copy Serialization:** Implements `rkyv` for efficient, zero-copy data access directly from memory-mapped files.
* **Range Scans:** Supports efficient iteration over keys within a specified range.
* **Asynchronous I/O:** Built on the `tokio` runtime for non-blocking operations and high concurrency.

## Architecture

### Write Path
1.  **WAL Append:** Every write operation (`put`, `delete`) is first appended to the Write-Ahead Log on disk for durability.
2.  **MemTable Insert:** Data is then inserted into the in-memory SkipList.
3.  **Flush:** When a threshold is met, the MemTable is flushed to disk as a Sorted String Table (SST) at Level 0, and the WAL is reset.

### Read Path
1.  **MemTable Lookup:** The engine first checks the active in-memory buffer.
2.  **SSTable Lookup:** If not found, the engine queries SSTables starting from Level 0 down through the hierarchy.
3.  **Optimizations:**
    * **Bloom Filters:** Probabilistic data structures used to skip files that do not contain the target key.
    * **Sparse Index:** Locates the specific 4KB data block containing the key to minimize disk reads.

### Compaction Strategy
The engine implements a tiered/leveled compaction algorithm to manage disk space and read latency:
* **Level 0 (L0):** Buffer for flushed MemTables. Threshold: 2 files.
* **Level N (L1+):** Deeper storage levels with higher capacities. Threshold: 4 files.
* **Process:** When a level exceeds its threshold, all files in that level are merge-sorted into a single SSTable at the next level (`Level + 1`), removing overwritten keys and tombstones.

## Storage Format

### SSTable Structure (`.sst`)
SSTables are immutable and optimized for memory mapping (`mmap`):
* **Data Blocks:** 4KB chunks containing sorted key-value pairs.
* **Meta Block:** Contains the Block Index (first keys and offsets) and Bloom Filter.
* **Footer:** Fixed-size 8-byte trailer containing the offset of the Meta Block.

### Manifest (`manifest.json`)
The Manifest tracks the active file set and their hierarchy levels.

```json
{
  "files": [
    {
      "id": 1770295102,
      "level": 1
    },
    {
      "id": 1770295146,
      "level": 2
    }
  ]
}
```

## Setup and Usage

### Prerequisites
* Rust (latest stable toolchain)

### Build and Run

```bash
# Build release version
cargo build --release

# Run the CLI
cargo run
```

### CLI Commands

| Command | Usage | Description |
| :--- | :--- | :--- |
| `put` | `put [key] [value]` | Insert or update a key-value pair. |
| `get` | `get [key]` | Retrieve the value for a specific key. |
| `delete` | `delete [key]` | Mark a key for deletion (writes a tombstone). |
| `scan` | `scan [start] [end]` | Iterate over keys in the range `[start, end)`. |
| `flush` | `flush` | Manually persist the current MemTable to Level 0. |
| `compact` | `compact` | Manually trigger the leveled compaction process. |
| `exit` | `exit` | Gracefully shutdown the engine. |

## Project Structure

* `src/engine.rs`: Core engine implementation (Put, Get, Scan, Flush, Compact).
* `src/memtable.rs`: Concurrent in-memory SkipList implementation.
* `src/sstable/builder.rs`: Logic for constructing block-based SST files.
* `src/sstable/reader.rs`: Memory-mapped reading and indexing logic.
* `src/manifest.rs`: Atomic database state management.
* `src/wal.rs`: Write-Ahead Log for crash resilience.
* `src/merge.rs`: K-way merge sorting for compaction and scanning.

## Dependencies

* `tokio`: Asynchronous runtime for I/O and task scheduling.
* `crossbeam-skiplist`: High-performance concurrent SkipList.
* `memmap2`: Efficient memory mapping for file access.
* `rkyv`: Zero-copy serialization framework.
* `bloomfilter`: Read-path optimization structure.
* `serde_json`: Manifest serialization and deserialization.
