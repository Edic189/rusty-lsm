use crate::error::Result;
use crate::manifest::ManifestManager;
use crate::memtable::MemTable;
use crate::merge::MergeIterator;
use crate::sstable::{builder::SstBuilder, reader::SstReader};
use crate::wal::Wal;
use bytes::Bytes;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{Mutex, RwLock};

pub struct StorageEngine {
    dir: PathBuf,
    wal: Wal,
    memtable: RwLock<Arc<MemTable>>,
    sstables: RwLock<Vec<Arc<SstReader>>>,
    manifest: Arc<Mutex<ManifestManager>>,
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
        // 1. Faza SELEKCIJE: Tražimo samo Level 0 datoteke
        let candidates: Vec<Arc<SstReader>>;
        let candidate_ids: Vec<u128>;

        {
            // Zaključamo manifest samo kratko da vidimo što imamo
            let manifest = self.manifest.lock().await;
            let active_files = manifest.get_active_files();

            // Filtriramo samo one koje su na Levelu 0
            let l0_ids: Vec<u128> = active_files
                .iter()
                .filter(|f| f.level == 0)
                .map(|f| f.id)
                .collect();

            // Prag: Pokreni kompakciju samo ako imamo barem 2 L0 datoteke
            if l0_ids.len() < 2 {
                tracing::info!(
                    "Skipping compaction: only {} L0 files (need 2+)",
                    l0_ids.len()
                );
                return Ok(());
            }

            // Sada nađemo stvarne SstReadere za te ID-eve
            let guard = self.sstables.read().await;
            candidates = guard
                .iter()
                .filter(|sst| {
                    // Izvlačimo ID iz imena datoteke
                    let id = sst
                        .path
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse::<u128>()
                        .unwrap();
                    l0_ids.contains(&id)
                })
                .cloned()
                .collect();

            candidate_ids = l0_ids;
        }

        tracing::info!("Starting compaction of {} L0 files...", candidates.len());

        let dir_clone = self.dir.clone();
        let candidate_paths: Vec<PathBuf> = candidates.iter().map(|c| c.path.clone()).collect();

        // 2. Faza IZVRŠENJA (Heavy lifting u threadu)
        let new_sst_path = tokio::task::spawn_blocking(move || {
            Self::run_compaction_logic(candidate_paths, dir_clone)
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

        // 3. Faza AŽURIRANJA (Manifest)
        {
            let mut manifest = self.manifest.lock().await;
            // Rezultat L0 kompakcije ide u Level 1
            manifest.add_file(new_id, 1).await?;
            // Mičemo samo one L0 datoteke koje smo spojili
            manifest.remove_files(&candidate_ids).await?;
        }

        // 4. Faza AŽURIRANJA (Memorija / SSTables)
        {
            let mut guard = self.sstables.write().await;
            // Izbaci stare readere iz memorije
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

            // Dodaj novi reader
            let new_reader = SstReader::open(&new_sst_path)?;
            guard.push(Arc::new(new_reader));

            // Bitno: Sortiramo listu readera (stariji -> noviji) da bi Get radio ispravno
            guard.sort_by_key(|r| {
                r.path
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse::<u128>()
                    .unwrap()
            });
        }

        // 5. Cleanup (Brisanje s diska)
        for reader in candidates {
            let _ = fs::remove_file(&reader.path).await;
        }

        tracing::info!(
            "Compaction complete. {} L0 files merged into Level 1: {:?}",
            candidate_ids.len(),
            new_sst_path
        );
        Ok(())
    }

    fn run_compaction_logic(files: Vec<PathBuf>, dir: PathBuf) -> Result<PathBuf> {
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
        rt.block_on(async { builder.build(merge_iter, 0).await })?;

        Ok(new_path)
    }
}
