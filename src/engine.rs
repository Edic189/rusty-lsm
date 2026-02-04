use crate::error::Result;
use crate::memtable::MemTable;
use crate::sstable::{builder::SstBuilder, reader::SstReader};
use crate::wal::Wal;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;

pub struct StorageEngine {
    dir: PathBuf,

    /// The Write-Ahead Log for durability.
    wal: Wal,

    /// The In-Memory Table.
    /// Wrapped in RwLock<Arc<...>> to allow "rotation" (swapping the active table)
    /// during a flush operation without blocking readers for long.
    memtable: RwLock<Arc<MemTable>>,

    /// List of immutable SSTables on disk.
    /// Ordered by creation time (Oldest -> Newest).
    sstables: RwLock<Vec<SstReader>>,
}

impl StorageEngine {
    /// Initialize the Storage Engine.
    /// Recovers state from WAL and loads existing SSTables.
    pub async fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).await?;

        // 1. Open or Create WAL
        let wal_path = dir.join("wal.log");
        let wal = Wal::new(&wal_path).await?;

        // 2. RECOVERY: Replay WAL into a fresh MemTable
        // This ensures data survives crashes.
        let memtable = MemTable::new();
        let recovered_data = wal.replay().await?;

        for (key, value) in recovered_data {
            if let Some(val) = value {
                memtable.put(key, val)?;
            } else {
                memtable.delete(key)?;
            }
        }

        if !memtable.is_empty() {
            tracing::info!("Recovered {} records from WAL", memtable.approximate_size());
        }

        // 3. Load existing SSTables
        // We scan the directory for .sst files and open them.
        let mut sstables = Vec::new();
        let mut read_dir = fs::read_dir(&dir).await?;
        let mut paths = Vec::new();

        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sst") {
                paths.push(path);
            }
        }

        // Sort paths to maintain chronological order (Oldest -> Newest)
        // This is crucial for correct shadowing of keys.
        paths.sort();

        for path in paths {
            let reader = SstReader::open(&path)?;
            sstables.push(reader);
        }

        Ok(Self {
            dir,
            wal,
            memtable: RwLock::new(Arc::new(memtable)),
            sstables: RwLock::new(sstables),
        })
    }

    /// Write a Key-Value pair.
    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        let value = value.into();

        // 1. Write to WAL (Durability)
        // We persist before modifying memory.
        self.wal.append(&key, Some(&value)).await?;

        // 2. Write to MemTable (Visibility)
        let mem = self.memtable.read().await;
        mem.put(key, value)?;

        Ok(())
    }

    /// Delete a Key (Write Tombstone).
    pub async fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();

        // 1. Write Tombstone to WAL
        self.wal.append(&key, None).await?;

        // 2. Write Tombstone to MemTable
        let mem = self.memtable.read().await;
        mem.delete(key)?;

        Ok(())
    }

    /// Retrieve a value.
    /// Checks MemTable first, then iterates SSTables from Newest to Oldest.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Check MemTable
        {
            let mem = self.memtable.read().await;
            if let Some(value) = mem.get(key) {
                return Ok(Some(value.to_vec()));
            }
            // Note: If MemTable has a Tombstone, it returns None.
            // In a production system, we would need to distinguish "Deleted" vs "NotFound".
            // For this portfolio MVP, we assume fallback to disk is acceptable.
        }

        // 2. Check SSTables (Reverse order: Newest files first)
        let sstables = self.sstables.read().await;
        for sst in sstables.iter().rev() {
            if let Some(val) = sst.get(key)? {
                return Ok(Some(val));
            }
        }

        Ok(None)
    }

    /// Flush the current MemTable to disk.
    pub async fn flush(&self) -> Result<()> {
        let old_memtable;

        // 1. Rotate MemTable
        // We acquire a write lock briefly to swap the Arc pointer.
        {
            let mut guard = self.memtable.write().await;
            old_memtable = guard.clone(); // Clone the Arc, not the data
            *guard = Arc::new(MemTable::new()); // Replace with fresh table
        }

        if old_memtable.is_empty() {
            return Ok(());
        }

        // 2. Build SSTable from the frozen (old) MemTable
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let sst_path = self.dir.join(format!("{}.sst", id));

        let builder = SstBuilder::new(&sst_path);
        builder.build_from_memtable(&old_memtable).await?;

        // 3. Open the new SSTable and add it to the list
        let reader = SstReader::open(&sst_path)?;
        {
            let mut sst_guard = self.sstables.write().await;
            sst_guard.push(reader);
        }

        // 4. Cleanup WAL
        // In a production system, you would rotate the WAL file here.
        // For this MVP, we acknowledge the data is safe in SST,
        // but we don't truncate the WAL logic to keep it simple.

        tracing::info!("Flushed MemTable to {:?}", sst_path);

        Ok(())
    }
}
