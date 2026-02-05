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
    wal: Wal,
    memtable: RwLock<Arc<MemTable>>,
    sstables: RwLock<Vec<SstReader>>,
}

impl StorageEngine {
    pub async fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).await?;

        let wal_path = dir.join("wal.log");
        let wal = Wal::new(&wal_path).await?;

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
            sstables.push(reader);
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
        // 1. Provjeri MemTable
        {
            let mem = self.memtable.read().await;
            match mem.get(key) {
                Some(Some(val)) => return Ok(Some(val.to_vec())), // Nađeno
                Some(None) => return Ok(None), // Nađen Tombstone -> PODATAK JE OBRISAN, STANI.
                None => {}                     // Nije u memoriji, traži dalje na disku
            }
        }

        // 2. Provjeri SSTables (od najnovije prema najstarijoj)
        let sstables = self.sstables.read().await;
        for sst in sstables.iter().rev() {
            // Ovdje također moramo paziti: ako SST vrati Tombstone, moramo stati!
            // Trenutni SstReader::get vraća Option<Vec<u8>>, što skriva Tombstone.
            // Za pravi fix, SstReader bi trebao vraćati enum, ali za sada
            // pretpostavljamo da ako SstReader vrati Ok(Some(v)), to je vrijednost.
            // (Napomena: Ovo je još jedna točka za buduće poboljšanje)
            if let Some(val) = sst.get(key)? {
                return Ok(Some(val));
            }
        }

        Ok(None)
    }

    pub async fn flush(&self) -> Result<()> {
        let old_memtable;

        // 1. Zamjena MemTable-a (brzi lock)
        {
            let mut guard = self.memtable.write().await;
            old_memtable = guard.clone();
            *guard = Arc::new(MemTable::new());
        }

        if old_memtable.is_empty() {
            return Ok(());
        }

        // 2. Zapiši SST na disk
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let sst_path = self.dir.join(format!("{}.sst", id));

        let builder = SstBuilder::new(&sst_path);
        builder.build_from_memtable(&old_memtable).await?;

        // 3. Dodaj u listu čitača
        let reader = SstReader::open(&sst_path)?;
        {
            let mut sst_guard = self.sstables.write().await;
            sst_guard.push(reader);
        }

        // 4. OČISTI WAL [NOVO]
        // Budući da su podaci sada sigurni u SST datoteci,
        // stari WAL nam više ne treba za oporavak.
        self.wal.reset().await?;

        tracing::info!("Flushed MemTable to {:?} and reset WAL", sst_path);

        Ok(())
    }
}
