use crate::error::Result;
use crate::memtable::MemTable;
use bloomfilter::Bloom;
use rkyv::{Archive, Deserialize, Serialize};
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};

#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive(check_bytes)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive(check_bytes)]
pub struct SstFileData {
    pub entries: Vec<SstEntry>,
    pub bloom_bytes: Vec<u8>,
}

pub struct SstBuilder {
    path: std::path::PathBuf,
}

impl SstBuilder {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// Generička metoda koja gradi SST iz bilo kojeg iteratora (ključ, vrijednost)
    pub async fn build<I>(self, iter: I, approx_items: usize) -> Result<()>
    where
        I: Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    {
        let mut entries = Vec::new();
        // 1. Bloom Filter
        let mut bloom = Bloom::new_for_fp_rate(approx_items.max(100), 0.01);

        for (key, value) in iter {
            bloom.set(&key);
            entries.push(SstEntry { key, value });
        }

        // 2. Serijaliziraj Bloom
        let bloom_bytes = serde_json::to_vec(&bloom)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

        // 3. Pripremi podatke
        let file_data = SstFileData {
            entries,
            bloom_bytes,
        };

        // 4. Serijaliziraj podatke (Zero-copy format)
        let bytes = rkyv::to_bytes::<_, 4096>(&file_data)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

        // 5. Zapiši na disk
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .await?;

        let mut writer = BufWriter::new(file);
        writer.write_all(&bytes).await?;
        writer.flush().await?;
        writer.get_ref().sync_all().await?;

        Ok(())
    }

    /// Helper za MemTable (koristi novu generičku metodu)
    pub async fn build_from_memtable(self, memtable: &MemTable) -> Result<()> {
        let approx_items = memtable.approximate_size() / 50; // Gruba procjena

        // Pretvaramo MemTable iter u (Vec<u8>, Option<Vec<u8>>)
        let iter = memtable.iter().map(|entry| {
            let key = entry.key().to_vec();
            let value = entry.value().as_ref().map(|b| b.to_vec());
            (key, value)
        });

        self.build(iter, approx_items).await
    }
}
