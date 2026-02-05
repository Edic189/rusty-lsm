use crate::error::Result;
use crate::manifest::ManifestManager; // <--- NOVO
use crate::memtable::MemTable;
use crate::merge::MergeIterator;
use crate::sstable::{builder::SstBuilder, reader::SstReader};
use crate::wal::Wal;
use bytes::Bytes;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{Mutex, RwLock}; // Treba nam Mutex za manifest

pub struct StorageEngine {
    dir: PathBuf,
    wal: Wal,
    memtable: RwLock<Arc<MemTable>>,
    sstables: RwLock<Vec<Arc<SstReader>>>,
    manifest: Arc<Mutex<ManifestManager>>, // <--- NOVO: Čuva stanje
}

impl StorageEngine {
    pub async fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).await?;

        let wal_path = dir.join("wal.log");
        let wal = Wal::new(&wal_path).await?;

        // 1. Učitaj Manifest
        let manifest = ManifestManager::new(&dir).await?;
        let active_files = manifest.get_active_files();

        let memtable = MemTable::new();
        // Replay WAL
        let recovered_data = wal.replay().await?;
        for (key, value) in recovered_data {
            if let Some(val) = value {
                memtable.put(key, val)?;
            } else {
                memtable.delete(key)?;
            }
        }

        // 2. Učitaj SAMO one SST datoteke koje su u Manifestu
        let mut sstables = Vec::new();
        for meta in active_files {
            let sst_path = dir.join(format!("{}.sst", meta.id));
            if sst_path.exists() {
                let reader = SstReader::open(&sst_path)?;
                sstables.push(Arc::new(reader));
            }
        }

        Ok(Self {
            dir,
            wal,
            memtable: RwLock::new(Arc::new(memtable)),
            sstables: RwLock::new(sstables),
            manifest: Arc::new(Mutex::new(manifest)),
        })
    }

    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        let value = value.into();
        self.wal.append(&key, Some(&value)).await?;
        let mem = self.memtable.read().await;
        mem.put(key, value)?;
        Ok(())
    }

    pub async fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        self.wal.append(&key, None).await?;
        let mem = self.memtable.read().await;
        mem.delete(key)?;
        Ok(())
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
        // Čitamo od najnovije (zadnje u vektoru) prema najstarijoj
        for sst in sstables.iter().rev() {
            if let Some(val) = sst.get(key)? {
                return Ok(Some(val));
            }
        }

        Ok(None)
    }

    pub async fn scan(
        &self,
        range: impl std::ops::RangeBounds<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start_bound = range.start_bound();
        let end_bound = range.end_bound();

        let start_slice = match start_bound {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_slice = match end_bound {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let mem_guard = self.memtable.read().await;
        let mem_iter = mem_guard
            .scan(start_slice, end_slice)
            .map(|(k, v)| (k.to_vec(), v.map(|val| val.to_vec())));

        let sst_guard = self.sstables.read().await;
        let mut iterators = Vec::new();
        for sst in sst_guard.iter() {
            // FIX: Dodan lifetime cast da umirimo compiler oko Box-a
            let iter = sst
                .scan(start_slice)
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec())));
            iterators.push(Box::new(iter) as Box<dyn Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>>);
        }

        iterators.push(Box::new(mem_iter));
        let merge_iter = MergeIterator::new(iterators);

        let mut results = Vec::new();
        for (key, value) in merge_iter {
            let is_past_end = match end_slice {
                Bound::Included(end) => key.as_slice() > end,
                Bound::Excluded(end) => key.as_slice() >= end,
                Bound::Unbounded => false,
            };
            if is_past_end {
                break;
            }
            if let Some(val) = value {
                results.push((key, val));
            }
        }
        Ok(results)
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
            .as_micros(); // ID datoteke

        let sst_path = self.dir.join(format!("{}.sst", id));

        let builder = SstBuilder::new(&sst_path);
        builder.build_from_memtable(&old_memtable).await?;

        // 1. Zapiši u Manifest da imamo novu datoteku (Level 0)
        {
            let mut manifest = self.manifest.lock().await;
            manifest.add_file(id, 0).await?;
        }

        // 2. Dodaj u memoriju
        let reader = SstReader::open(&sst_path)?;
        {
            let mut sst_guard = self.sstables.write().await;
            sst_guard.push(Arc::new(reader));
        }

        self.wal.reset().await?;
        tracing::info!("Flushed MemTable to Level 0: {:?}", sst_path);
        Ok(())
    }

    pub async fn compact(&self) -> Result<()> {
        let candidates: Vec<Arc<SstReader>>;
        let candidate_ids: Vec<u128>;
        {
            let guard = self.sstables.read().await;
            if guard.len() <= 1 {
                return Ok(());
            }
            candidates = guard.clone();

            // Izvuci ID iz patha (malo hacky, ali radi za sada)
            // Pretpostavka: filename je "123456789.sst"
            candidate_ids = candidates
                .iter()
                .map(|c| {
                    c.path
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse::<u128>()
                        .unwrap()
                })
                .collect();
        }

        tracing::info!("Compacting {} files...", candidates.len());

        let dir_clone = self.dir.clone();
        // Moramo klonirati pathove za thread
        let candidate_paths: Vec<PathBuf> = candidates.iter().map(|c| c.path.clone()).collect();

        // Izvrši merge u threadu
        let new_sst_path = tokio::task::spawn_blocking(move || {
            Self::run_compaction_logic(candidate_paths, dir_clone)
        })
        .await
        .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))??;

        // Novi ID iz patha
        let new_id = new_sst_path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<u128>()
            .unwrap();

        // 3. Ažuriraj Manifest (Atomski swap)
        {
            let mut manifest = self.manifest.lock().await;
            // Dodaj novu (Level 1 - jer je compacted)
            manifest.add_file(new_id, 1).await?;
            // Obriši stare
            manifest.remove_files(&candidate_ids).await?;
        }

        // 4. Ažuriraj memoriju
        {
            let mut guard = self.sstables.write().await;
            // Makni stare
            guard.retain(|sst| {
                let id = sst
                    .path
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse::<u128>()
                    .unwrap();
                !candidate_ids.contains(&id)
            });
            // Dodaj novu
            let new_reader = SstReader::open(&new_sst_path)?;
            guard.push(Arc::new(new_reader));
        }

        // 5. Obriši stare datoteke s diska
        for reader in candidates {
            let _ = fs::remove_file(&reader.path).await;
        }

        tracing::info!(
            "Compaction complete. Created Level 1 SST: {:?}",
            new_sst_path
        );
        Ok(())
    }

    fn run_compaction_logic(files: Vec<PathBuf>, dir: PathBuf) -> Result<PathBuf> {
        // ... (Kod ostaje isti kao prije, samo ga trebaš ostaviti tu)
        // Kopiraj logiku iz prošlog odgovora za run_compaction_logic
        // Samo pazi da koristiš isti "id" generiranje kao u flushu
        let mut readers = Vec::new();
        for path in &files {
            let reader = SstReader::open(path)?;
            readers.push(reader);
        }

        let mut iterators = Vec::new();
        for reader in &readers {
            let iter = reader
                .iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec())));
            iterators.push(Box::new(iter) as Box<dyn Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>>);
        }

        let merge_iter = MergeIterator::new(iterators);

        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let new_path = dir.join(format!("{}.sst", id));

        let builder = SstBuilder::new(&new_path);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async { builder.build(merge_iter, 0).await })?; // approx_items 0 is fine

        Ok(new_path)
    }
}
