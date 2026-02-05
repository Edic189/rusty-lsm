use crate::error::Result;
use crate::memtable::MemTable;
use crate::merge::MergeIterator;
use crate::sstable::{builder::SstBuilder, reader::SstReader};
use crate::wal::Wal;
use bytes::Bytes;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;

pub struct StorageEngine {
    dir: PathBuf,
    wal: Wal,
    memtable: RwLock<Arc<MemTable>>,
    // Sstables su sada Arc kako bi ih lako klonirali za čitanje
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

        // Učitaj SST datoteke
        let mut sstables = Vec::new();
        let mut read_dir = fs::read_dir(&dir).await?;
        let mut paths = Vec::new();

        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sst") {
                paths.push(path);
            }
        }
        paths.sort(); // Bitno: starije datoteke prve

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
        // 1. Check Memtable
        {
            let mem = self.memtable.read().await;
            match mem.get(key) {
                Some(Some(val)) => return Ok(Some(val.to_vec())),
                Some(None) => return Ok(None), // Tombstone
                None => {}
            }
        }

        // 2. Check SSTables (od najnovije prema najstarijoj)
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

    /// --- BACKGROUND COMPACTION ---
    /// Ovo je ključna promjena. Ne blokiramo `write` lock cijelo vrijeme.
    pub async fn compact(&self) -> Result<()> {
        // 1. Identificiraj kandidate (Snapshot faza)
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

        // POPRAVAK 1: Kloniramo kandidate POSEBNO za thread.
        // Original varijablu `candidates` čuvamo za cleanup fazu dolje.
        let candidates_for_thread = candidates.clone();

        // 2. Izvrši spajanje u zasebnom threadu
        let new_sst_path = tokio::task::spawn_blocking(move || {
            Self::run_compaction_logic(candidates_for_thread, dir_clone)
        })
        .await
        .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))??;

        // 3. Zamjena (Critical Section)
        {
            let mut guard = self.sstables.write().await;

            // Koristimo 'candidates' (original) da znamo što micati
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

        // 4. Cleanup
        // Ovdje sada sigurno koristimo 'candidates' jer ga nismo poslali u thread
        for path in candidates {
            let _ = fs::remove_file(path).await;
        }

        tracing::info!("Compaction complete. New SST: {:?}", new_sst_path);
        Ok(())
    }

    fn run_compaction_logic(files: Vec<PathBuf>, dir: PathBuf) -> Result<PathBuf> {
        // 1. Otvori sve SstReadere odjednom
        let mut readers = Vec::new();
        for path in &files {
            let reader = SstReader::open(path)?;
            readers.push(reader);
        }

        // 2. Kreiraj iteratore za svaki reader
        // Moramo pretvoriti reference iz readera u (Vec<u8>, Option<Vec<u8>>)
        // jer MergeIterator treba 'owned' podatke za Heap.
        // Iako kopiramo ključeve, ovo je streaming, pa u memoriji držimo
        // samo po jedan ključ iz svake datoteke, a ne sve!
        let mut iterators = Vec::new();
        for reader in &readers {
            let iter = reader
                .iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec())));
            // Box-amo iteratore da riješimo tipove, jer svaki iter ima drugačiji lifetime
            // vezan za reader u vektoru. Ali ovdje su svi isti tip.
            // Jednostavnosti radi, collectamo ih u Vec<Box<dyn Iterator...>> ili koristimo isti tip.
            // Srećom, ovdje su svi isti tip (Map iterator), pa ne trebamo Box.
            iterators.push(iter);
        }

        // 3. Kreiraj MergeIterator
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

        // 4. Izgradi novu tablicu koristeći streaming iterator
        // Napomena: approx_items ovdje ne možemo znati točno unaprijed bez prolaska,
        // pa ćemo staviti 0 ili neku heuristiku. SstBuilder će raditi resize Blooma ili
        // možemo zbrojiti veličine datoteka za procjenu.
        let approx_items = 1000; // Ili zbroji .len() iz metadataka readera ako ih imaš

        rt.block_on(async { builder.build(merge_iter, approx_items).await })?;

        Ok(new_path)
    }
}
