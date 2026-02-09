use crate::batch::{BatchOp, WriteBatch};
use crate::cache::BlockCache;
use crate::config::LsmConfig;
use crate::error::Result;
use crate::iterator::StorageIterator;
use crate::manifest::ManifestManager;
use crate::memtable::MemTable;
use crate::merge::MergeIterator;
use crate::sstable::{builder::SstBuilder, reader::SstIterator, reader::SstReader};
use crate::wal::Wal;
use bytes::Bytes;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{Mutex, RwLock};

pub struct StorageEngine {
    pub config: LsmConfig,
    dir: PathBuf,
    wal: Wal,
    memtable: RwLock<Arc<MemTable>>,
    sstables: RwLock<Vec<Arc<SstReader>>>,
    manifest: Arc<Mutex<ManifestManager>>,
    block_cache: Arc<BlockCache>,
}

impl StorageEngine {
    pub async fn new(config: LsmConfig) -> Result<Self> {
        let dir = config.dir.clone();
        fs::create_dir_all(&dir).await?;

        let wal_path = dir.join("wal.log");
        let wal = Wal::new(&wal_path).await?;

        let manifest = ManifestManager::new(&dir).await?;
        let active_files = manifest.get_active_files();

        let memtable = MemTable::new();
        let recovered_data = wal.replay().await?;

        if !recovered_data.is_empty() {
            tracing::info!("Recovered {} records from WAL", recovered_data.len());
        }

        for (key, value) in recovered_data {
            if let Some(val) = value {
                memtable.put(key, val)?;
            } else {
                memtable.delete(key)?;
            }
        }

        let block_cache = Arc::new(BlockCache::new(config.block_cache_capacity));

        let mut sstables = Vec::new();
        for meta in active_files {
            let sst_path = dir.join(format!("{}.sst", meta.id));
            if sst_path.exists() {
                let reader = SstReader::open(&sst_path, meta.id, Some(block_cache.clone()))?;
                sstables.push(Arc::new(reader));
            }
        }

        sstables.sort_by_key(|r| r.id);

        Ok(Self {
            config,
            dir,
            wal,
            memtable: RwLock::new(Arc::new(memtable)),
            sstables: RwLock::new(sstables),
            manifest: Arc::new(Mutex::new(manifest)),
            block_cache,
        })
    }

    pub async fn write(&self, batch: WriteBatch) -> Result<()> {
        let should_flush = {
            let mem = self.memtable.read().await;
            mem.approximate_size() > self.config.memtable_capacity
        };

        if should_flush {
            tracing::info!("MemTable capacity reached. Triggering flush.");
            self.flush().await?;
        }

        self.wal.write_batch(&batch.operations).await?;

        let mem = self.memtable.read().await;
        for op in batch.operations {
            match op {
                BatchOp::Put(key, value) => {
                    mem.put(Bytes::from(key), Bytes::from(value))?;
                }
                BatchOp::Delete(key) => {
                    mem.delete(Bytes::from(key))?;
                }
            }
        }
        Ok(())
    }

    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(&key.into(), &value.into());
        self.write(batch).await
    }

    pub async fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(&key.into());
        self.write(batch).await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        {
            let mem = self.memtable.read().await;
            match mem.get(key) {
                Some(Some(val)) => return Ok(Some(val.to_vec())),
                Some(None) => return Ok(None),
                None => {}
            }
        }

        let sstables = self.sstables.read().await;
        for sst in sstables.iter().rev() {
            if let Some(val) = sst.get(key)? {
                return Ok(Some(val));
            }
        }

        Ok(None)
    }

    pub async fn flush(&self) -> Result<()> {
        let old_memtable;
        {
            let mut guard = self.memtable.write().await;
            old_memtable = guard.clone();
            *guard = Arc::new(MemTable::new());
        }

        if old_memtable.is_empty() {
            return Ok(());
        }

        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let sst_path = self.dir.join(format!("{}.sst", id));
        tracing::info!("Flushing MemTable to Level 0: {:?}", sst_path);

        let builder = SstBuilder::new(&sst_path, self.config.block_size);
        builder.build_from_memtable(&old_memtable).await?;

        {
            let mut manifest = self.manifest.lock().await;
            manifest.add_file(id, 0).await?;
        }

        let reader = SstReader::open(&sst_path, id, Some(self.block_cache.clone()))?;
        {
            let mut sst_guard = self.sstables.write().await;
            sst_guard.push(Arc::new(reader));
            sst_guard.sort_by_key(|r| r.id);
        }

        self.wal.reset().await?;

        Ok(())
    }

    pub async fn scan(
        &self,
        range: impl std::ops::RangeBounds<Vec<u8>>,
    ) -> Result<StorageIterator> {
        let start_bound = range.start_bound();
        let end_bound = range.end_bound();

        let start_bytes = match start_bound {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bytes = match end_bound {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let mem_vec: Vec<(Vec<u8>, Option<Vec<u8>>)> = {
            let mem_guard = self.memtable.read().await;
            mem_guard
                .scan(start_bytes, end_bytes)
                .map(|(k, v)| (k.to_vec(), v.map(|val| val.to_vec())))
                .collect()
        };
        let mem_iter = mem_vec.into_iter();

        let sst_guard = self.sstables.read().await;
        let mut iterators: Vec<Box<dyn Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>>> = Vec::new();

        for sst in sst_guard.iter() {
            let iter = SstIterator::new(sst.clone(), start_bytes, end_bytes);
            iterators.push(Box::new(iter));
        }

        iterators.push(Box::new(mem_iter));

        Ok(StorageIterator::new(iterators))
    }

    pub async fn compact(&self) -> Result<()> {
        let (target_level, candidate_ids) = {
            let manifest = self.manifest.lock().await;
            let active_files = manifest.get_active_files();

            let mut result = None;

            for level in 0..self.config.max_levels {
                let files_in_level: Vec<u128> = active_files
                    .iter()
                    .filter(|f| f.level == level)
                    .map(|f| f.id)
                    .collect();

                let threshold = if level == 0 {
                    self.config.l0_threshold
                } else {
                    self.config.ln_threshold
                };

                if files_in_level.len() >= threshold {
                    result = Some((level, files_in_level));
                    break;
                }
            }
            match result {
                Some(res) => res,
                None => return Ok(()),
            }
        };

        let candidates: Vec<Arc<SstReader>> = {
            let guard = self.sstables.read().await;
            guard
                .iter()
                .filter(|sst| candidate_ids.contains(&sst.id))
                .cloned()
                .collect()
        };

        let next_level = target_level + 1;
        let is_last_level = next_level == self.config.max_levels - 1;

        tracing::info!(
            "Compacting Level {} -> Level {} ({} files)",
            target_level,
            next_level,
            candidates.len()
        );

        let dir_clone = self.dir.clone();
        let candidate_paths: Vec<(PathBuf, u128)> =
            candidates.iter().map(|c| (c.path.clone(), c.id)).collect();

        let block_size = self.config.block_size;

        let new_sst_path = tokio::task::spawn_blocking(move || {
            Self::run_compaction_logic(candidate_paths, dir_clone, is_last_level, block_size)
        })
        .await
        .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))??;

        let new_id = new_sst_path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<u128>()
            .unwrap();

        {
            let mut manifest = self.manifest.lock().await;
            manifest.add_file(new_id, next_level).await?;
            manifest.remove_files(&candidate_ids).await?;
        }

        {
            let mut guard = self.sstables.write().await;
            guard.retain(|sst| !candidate_ids.contains(&sst.id));
            let new_reader =
                SstReader::open(&new_sst_path, new_id, Some(self.block_cache.clone()))?;
            guard.push(Arc::new(new_reader));
            guard.sort_by_key(|r| r.id);
        }

        for reader in candidates {
            let _ = fs::remove_file(&reader.path).await;
        }

        tracing::info!("Compaction complete");

        Ok(())
    }

    fn run_compaction_logic(
        files: Vec<(PathBuf, u128)>,
        dir: PathBuf,
        is_last_level: bool,
        block_size: usize,
    ) -> Result<PathBuf> {
        let mut readers = Vec::new();
        for (path, id) in &files {
            let reader = Arc::new(SstReader::open(path, *id, None)?);
            readers.push(reader);
        }

        let mut iterators: Vec<Box<dyn Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>>> = Vec::new();
        for reader in &readers {
            let iter = SstIterator::new(reader.clone(), Bound::Unbounded, Bound::Unbounded);
            iterators.push(Box::new(iter));
        }

        let merge_iter = MergeIterator::new(iterators);

        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let new_path = dir.join(format!("{}.sst", id));

        let builder = SstBuilder::new(&new_path, block_size);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async { builder.build(merge_iter, is_last_level).await })?;

        Ok(new_path)
    }

    pub async fn stats(&self) -> String {
        let mem = self.memtable.read().await;
        let sstables = self.sstables.read().await;
        let manifest = self.manifest.lock().await;

        let mem_size = mem.approximate_size();
        let sst_count = sstables.len();
        let active_files_count = manifest.get_active_files().len();

        let mut levels = vec![0; self.config.max_levels as usize];
        for meta in manifest.get_active_files() {
            if (meta.level as usize) < levels.len() {
                levels[meta.level as usize] += 1;
            }
        }

        format!(
            "--- DB Stats ---\n\
             MemTable Size: {:.2} MB\n\
             SSTables (Active): {}\n\
             SSTables (Loaded): {}\n\
             Level Distribution: {:?}\n\
             ----------------",
            mem_size as f64 / 1024.0 / 1024.0,
            active_files_count,
            sst_count,
            levels
        )
    }
}
