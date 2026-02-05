use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileMetadata {
    pub id: u128,   // Jedinstveni ID datoteke (timestamp)
    pub level: u32, // Razina u LSM stablu (0, 1, 2...)
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Manifest {
    pub files: HashSet<FileMetadata>,
}

pub struct ManifestManager {
    path: PathBuf,
    pub state: Manifest,
}

impl ManifestManager {
    pub async fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let path = dir.as_ref().join("manifest.json");
        let state = if path.exists() {
            let mut file = fs::File::open(&path).await?;
            let mut content = Vec::new();
            file.read_to_end(&mut content).await?;
            serde_json::from_slice(&content).unwrap_or_default()
        } else {
            Manifest::default()
        };

        Ok(Self { path, state })
    }

    /// Dodaje novu SST tablicu u manifest
    pub async fn add_file(&mut self, id: u128, level: u32) -> Result<()> {
        self.state.files.insert(FileMetadata { id, level });
        self.flush().await
    }

    /// Uklanja tablice iz manifesta (nakon kompakcije)
    pub async fn remove_files(&mut self, ids: &[u128]) -> Result<()> {
        self.state.files.retain(|f| !ids.contains(&f.id));
        self.flush().await
    }

    // Atomski zapis na disk
    async fn flush(&self) -> Result<()> {
        let content = serde_json::to_vec_pretty(&self.state)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

        // Pišemo u privremenu datoteku pa preimenujemo (Atomic Save)
        let tmp_path = self.path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;

        file.write_all(&content).await?;
        file.flush().await?;

        // Atomski rename osigurava da manifest nikad nije korumpiran
        fs::rename(tmp_path, &self.path).await?;
        Ok(())
    }

    pub fn get_active_files(&self) -> Vec<FileMetadata> {
        let mut files: Vec<_> = self.state.files.iter().cloned().collect();
        // Sortiramo po ID-u (vremenu) da imamo kronološki red
        files.sort_by_key(|f| f.id);
        files
    }
}
