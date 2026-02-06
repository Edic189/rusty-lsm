# Rusty LSM: High-Performance Async Key-Value Store

Rusty LSM is a production-grade, persistent Log-Structured Merge-Tree (LSM) storage engine written in Rust. It is built for high-throughput write operations and low-latency reads, leveraging asynchronous I/O via Tokio and modern storage techniques including memory mapping, zero-copy deserialization, and block compression.

## Key Features

* **LSM-Tree Architecture:** Optimized for sequential writes and high ingestion rates.
* **Data Durability:** Write-Ahead Log (WAL) with CRC32 checksums ensures zero data loss on crashes.
* **Atomic Transactions:** Support for Atomic Write Batches (Put/Delete multiple keys in a single transaction).
* **High-Performance Reads:**
    * **Memory Mapping (mmap):** Bypasses syscall overhead for file I/O.
    * **Block Cache (LRU):** Caches frequently accessed uncompressed data blocks in RAM.
    * **Bloom Filters:** Probabilistic filtering to avoid unnecessary disk seeks.
    * **Range Filtering:** Checks min/max key metadata before opening blocks.
* **Storage Efficiency:**
    * **Snappy Compression:** Blocks are compressed on disk using the Google Snappy algorithm.
    * **Prefix Compression:** Reduces space usage by storing only key differences for sorted data.
    * **Tombstone Garbage Collection:** Removes deleted keys during compaction at the bottom level.
* **Zero-Copy Access:** Uses `rkyv` for deserializing data directly from memory maps without allocations.
* **Streaming Iterators:** Memory-safe scan operations that yield items lazily, preventing OOM errors on large range scans.

## Architecture

### Write Path
1. **WAL Append:** Operations are serialized into a batch, checksummed (CRC32), and appended to the disk log.
2. **MemTable Insert:** Data is applied to the concurrent in-memory SkipList.
3. **Flush:** When MemTable size exceeds the limit, it is flushed to L0 SSTable using Prefix Encoding and Snappy Compression.

### Read Path
1. **MemTable:** Checks active memory buffer.
2. **Block Cache:** Checks the in-memory LRU cache for the requested block.
3. **SSTable Lookup:**
    * **Bloom Filter & Range Check:** Fast rejection if key is not present.
    * **Sparse Index:** Binary search to locate the 4KB data block.
    * **Fetch & Verify:** Reads from disk/mmap, verifies CRC32, decompresses, and populates Cache.

### Compaction Strategy
* **Leveled Compaction:** Moves data through levels (L0 -> L1 -> ... -> L7).
* **Merge Sort:** Merges overlapping files using a k-way merge iterator.
* **GC:** Tombstones (deletes) are physically removed when data reaches the bottom level.

## Storage Format

### SSTable (.sst)
Files are immutable and organized into 4KB blocks:
* **Data Block:** [Overlap | DiffKey | Value] entries -> Compressed (Snappy) -> Checksummed (CRC32).
* **Meta Block:** Sparse Index, Bloom Filter, Min/Max Keys.
* **Footer:** Offset pointer to the Meta Block.

### Manifest (manifest.json)
Atomic state management tracking active files and levels, ensuring consistency across restarts.

## Setup and Usage

### Library Usage
Add `rusty-lsm` to your project dependencies.

```rust
use rusty_lsm::engine::StorageEngine;
use rusty_lsm::batch::WriteBatch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize engine
    let engine = StorageEngine::new("./db_data").await?;

    // 1. Simple Put/Get
    engine.put("user:1", "Alice").await?;
    
    if let Some(val) = engine.get(b"user:1").await? {
        println!("Found: {}", String::from_utf8_lossy(&val));
    }

    // 2. Atomic Batch Write
    let mut batch = WriteBatch::new();
    batch.put(b"user:2", b"Bob");
    batch.put(b"user:3", b"Charlie");
    batch.delete(b"user:1"); // Delete Alice
    engine.write(batch).await?;

    // 3. Range Scan (Streaming)
    // .to_vec() is required because scan expects Vec bounds
    let iter = engine.scan(b"user:0".to_vec()..b"user:9".to_vec()).await?;
    for (key, val) in iter {
        println!(
            "{} => {}", 
            String::from_utf8_lossy(&key), 
            String::from_utf8_lossy(&val)
        );
    }

    // 4. Manual Maintenance (Optional)
    engine.flush().await?;   // Force MemTable to Disk
    engine.compact().await?; // Trigger Compaction/GC

    Ok(())
}
```

### CLI Tool
You can run the engine as a standalone CLI.

```bash
cargo run --release --bin rusty-lsm
```

| Command | Usage | Description |
| :--- | :--- | :--- |
| `put` | `put [k] [v]` | Insert key-value. |
| `get` | `get [k]` | Retrieve value. |
| `delete` | `delete [k]` | Delete key. |
| `scan` | `scan [start] [end]` | Range scan (inclusive start, exclusive end). |
| `flush` | `flush` | Persist memory to disk. |
| `compact` | `compact` | Merge files and clean up deleted keys. |

## Project Structure

* `src/engine.rs`: Orchestrator (Put, Get, Scan, Compact).
* `src/sstable/`: Disk format, Builder (Compression/Prefix), Reader (mmap/access).
* `src/wal.rs`: Crash recovery log with Batch support.
* `src/memtable.rs`: Lock-free SkipList wrapper.
* `src/cache.rs`: Thread-safe LRU Block Cache.
* `src/batch.rs`: Atomic write batch definition.
* `src/iterator.rs`: Merging iterator for Scan operations.
* `src/manifest.rs`: Metadata and level management.

## Dependencies

* `tokio`
* `memmap2`
* `bytes`
* `lru`
* `rkyv`
* `serde`
* `bincode`
* `snap`
* `crc32fast`
* `bloomfilter`
* `crossbeam-skiplist`
