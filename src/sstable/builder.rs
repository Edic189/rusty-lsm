use crate::error::Result;
use crate::memtable::MemTable;
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

pub struct SstBuilder {
    path: std::path::PathBuf,
}

impl SstBuilder {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub async fn build_from_memtable(self, memtable: &MemTable) -> Result<()> {
        let mut entries = Vec::new();

        for entry in memtable.iter() {
            let key = entry.key().to_vec();
            let value = entry.value().as_ref().map(|b| b.to_vec());

            entries.push(SstEntry { key, value });
        }

        let bytes = rkyv::to_bytes::<_, 4096>(&entries)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

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

        tracing::info!(
            "Flushed SSTable to {:?} with {} entries",
            self.path,
            entries.len()
        );

        Ok(())
    }
}
