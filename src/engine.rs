use crate::error::Result;
use crate::memtable::MemTable;
use crate::sstable::{builder::SstBuilder, reader::SstReader};
use crate::wal::Wal;
use bytes::Bytes;
use std::collections::BTreeMap; // Treba nam za sortiranje pri kompaktu
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

        let builder = SstBuilder::new(&sst_path);
        builder.build_from_memtable(&old_memtable).await?;

        let reader = SstReader::open(&sst_path)?;
        {
            let mut sst_guard = self.sstables.write().await;
            sst_guard.push(reader);
        }

        self.wal.reset().await?;

        tracing::info!("Flushed MemTable to {:?}", sst_path);

        Ok(())
    }

    /// NOVO: Kompaktiranje
    /// Spaja sve SST datoteke u jednu, uklanja duplikate i obrisane podatke.
    pub async fn compact(&self) -> Result<()> {
        // 1. Uzimamo WRITE lock jer ćemo mijenjati strukturu sstables
        let mut sstables_guard = self.sstables.write().await;

        if sstables_guard.is_empty() {
            return Ok(());
        }

        tracing::info!("Starting compaction of {} tables...", sstables_guard.len());

        // 2. Mapiranje u memoriji (Sorted Map)
        // BTreeMap automatski sortira po ključu.
        // Ključ je Vec<u8>, Vrijednost je Option<Vec<u8>> (None znači tombstone)
        let mut map: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // 3. Iteriramo od NAJSTARIJE prema NAJNOVIJOJ tablici.
        // To osigurava da noviji podaci (čak i tombstones) pregaze stare.
        for sst in sstables_guard.iter() {
            for (key, value) in sst.iter() {
                map.insert(key.to_vec(), value.map(|v| v.to_vec()));
            }
        }

        // 4. Izradi novu SST datoteku
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let new_sst_path = self.dir.join(format!("{}_compacted.sst", id));

        // Moramo ručno napraviti MemTable ili adaptirati Builder.
        // Ovdje ćemo koristiti trik: Napraviti privremeni MemTable da iskoristimo postojeći Builder logic.
        // (U optimiziranoj verziji Builder bi primao Iterator, ali ovo je lakše za MVP)
        let temp_memtable = MemTable::new();
        for (key, value) in map {
            if let Some(val) = value {
                let _ = temp_memtable.put(Bytes::from(key), Bytes::from(val));
            } else {
                // Ako je zadnje stanje Tombstone, u Full Compaction ga možemo čak i izbaciti
                // ako smo sigurni da nema starijih snapshotova koji se koriste.
                // Za sigurnost, zadržimo ga kao delete marker.
                let _ = temp_memtable.delete(Bytes::from(key));
            }
        }

        let builder = SstBuilder::new(&new_sst_path);
        builder.build_from_memtable(&temp_memtable).await?;

        // 5. Otvori novu datoteku
        let new_reader = SstReader::open(&new_sst_path)?;

        // 6. Zamijeni listu sstables sa novom listom koja ima samo ovu jednu datoteku
        // Prvo skupimo putanje starih datoteka da ih možemo obrisati
        let old_files: Vec<PathBuf> = sstables_guard.iter().map(|r| r.path.clone()).collect();

        // Postavi novu listu
        *sstables_guard = vec![new_reader];

        // 7. Obriši stare datoteke s diska
        for path in old_files {
            let _ = tokio::fs::remove_file(path).await;
        }

        tracing::info!("Compaction complete. New SST: {:?}", new_sst_path);

        Ok(())
    }
}
