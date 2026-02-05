use crate::error::Result;
use crate::memtable::MemTable;
use crate::merge::MergeIterator;
use crate::sstable::{builder::SstBuilder, reader::SstReader};
use crate::wal::Wal;
use bytes::Bytes;
use std::collections::HashSet;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;

pub struct StorageEngine {
    dir: PathBuf,
    wal: Wal,
    memtable: RwLock<Arc<MemTable>>,
    sstables: RwLock<Vec<Arc<SstReader>>>,
}

impl StorageEngine {
    pub async fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).await?;

        let wal_path = dir.join("wal.log");
        let wal = Wal::new(&wal_path).await?;

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

        let mut sstables = Vec::new();
        let mut read_dir = fs::read_dir(&dir).await?;
        let mut paths = Vec::new();

        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sst") {
                paths.push(path);
            }
        }
        paths.sort();

        for path in paths {
            let reader = SstReader::open(&path)?;
            sstables.push(Arc::new(reader));
        }

        Ok(Self {
            dir,
            wal,
            memtable: RwLock::new(Arc::new(memtable)),
            sstables: RwLock::new(sstables),
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
        for sst in sstables.iter().rev() {
            if let Some(val) = sst.get(key)? {
                return Ok(Some(val));
            }
        }

        Ok(None)
    }

    /// SCAN: Vraća iterator sortiranih ključeva i vrijednosti u zadanom rasponu.
    /// Range argumenti: .. (sve), "a".."b" (od a do b), "a".. (od a do kraja)
    pub async fn scan(
        &self,
        range: impl std::ops::RangeBounds<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start_bound = range.start_bound();
        let end_bound = range.end_bound();

        // Konverzija Bound<&Vec<u8>> u Bound<&[u8]> za SST/Memtable pozive
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

        // 1. MemTable Iterator
        let mem_guard = self.memtable.read().await;
        let mem_iter = mem_guard
            .scan(start_slice, end_slice)
            .map(|(k, v)| (k.to_vec(), v.map(|val| val.to_vec())));

        // 2. SSTable Iterators
        let sst_guard = self.sstables.read().await;
        let mut iterators = Vec::new();

        // MemTable je uvijek najnoviji (index 0 u MergeIteratoru), pa ga dodajemo zadnjeg u listu
        // jer MergeIterator radi s heapom. Zapravo, MergeIterator uzima Vec iteratora.
        // Redoslijed u vectoru je bitan za deduplikaciju: iterator s VEĆIM indeksom u vectoru je "noviji".
        // Dakle, stare SST tablice idu na početak, a MemTable na kraj.

        for sst in sst_guard.iter() {
            let iter = sst
                .scan(start_slice)
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec())));
            // Moramo "zapakirati" iteratore da budu istog tipa (Box<dyn...>)
            iterators.push(Box::new(iter) as Box<dyn Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>>);
        }

        // Dodaj MemTable iterator kao zadnji (najnoviji podaci)
        iterators.push(Box::new(mem_iter));

        let merge_iter = MergeIterator::new(iterators);

        // 3. Filtriranje i limitiranje
        let mut results = Vec::new();

        for (key, value) in merge_iter {
            // Provjera gornje granice (End Bound)
            let is_past_end = match end_slice {
                Bound::Included(end) => key.as_slice() > end,
                Bound::Excluded(end) => key.as_slice() >= end,
                Bound::Unbounded => false,
            };

            if is_past_end {
                break;
            }

            // Ako je value Some, to je važeći podatak. Ako je None, to je tombstone (obrisano).
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
            .as_micros();
        let sst_path = self.dir.join(format!("{}.sst", id));

        let builder = SstBuilder::new(&sst_path);
        builder.build_from_memtable(&old_memtable).await?;

        let reader = SstReader::open(&sst_path)?;
        {
            let mut sst_guard = self.sstables.write().await;
            sst_guard.push(Arc::new(reader));
        }

        self.wal.reset().await?;
        tracing::info!("Flushed MemTable to {:?}", sst_path);
        Ok(())
    }

    pub async fn compact(&self) -> Result<()> {
        let candidates: Vec<PathBuf>;
        {
            let guard = self.sstables.read().await;
            if guard.len() <= 1 {
                return Ok(());
            }
            candidates = guard.iter().map(|r| r.path.clone()).collect();
        }

        tracing::info!(
            "Starting background compaction of {} files...",
            candidates.len()
        );

        let dir_clone = self.dir.clone();
        let candidates_for_thread = candidates.clone();

        let new_sst_path = tokio::task::spawn_blocking(move || {
            Self::run_compaction_logic(candidates_for_thread, dir_clone)
        })
        .await
        .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))??;

        {
            let mut guard = self.sstables.write().await;
            let compacted_files_set: HashSet<PathBuf> = candidates.iter().cloned().collect();

            let mut remaining_sstables: Vec<Arc<SstReader>> = guard
                .drain(..)
                .filter(|sst| !compacted_files_set.contains(&sst.path))
                .collect();

            let new_reader = SstReader::open(&new_sst_path)?;

            let mut new_list = vec![Arc::new(new_reader)];
            new_list.append(&mut remaining_sstables);

            *guard = new_list;
        }

        for path in candidates {
            let _ = fs::remove_file(path).await;
        }

        tracing::info!("Compaction complete. New SST: {:?}", new_sst_path);
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

            // Box-amo iteratore da budu isti tip
            iterators.push(Box::new(iter) as Box<dyn Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>>);
        }

        let merge_iter = MergeIterator::new(iterators);

        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let new_path = dir.join(format!("{}_compacted.sst", id));

        let builder = SstBuilder::new(&new_path);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let approx_items = 1000;

        rt.block_on(async { builder.build(merge_iter, approx_items).await })?;

        Ok(new_path)
    }
}
