// src/wal.rs (Final Clean Version)
use crate::error::Result;
use bytes::{BufMut, Bytes};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(Self {
            path,
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub async fn append(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let mut writer = self.writer.lock().await;

        let val_len = value.map(|v| v.len()).unwrap_or(0);
        let val_exists = value.is_some();

        let entry_size = 8 + 1 + 8 + key.len() + val_len;
        let mut buf = Vec::with_capacity(entry_size);

        buf.put_u64(key.len() as u64);
        if val_exists {
            buf.put_u8(1);
            buf.put_u64(val_len as u64);
        } else {
            buf.put_u8(0);
            buf.put_u64(0);
        }

        buf.put_slice(key);
        if let Some(v) = value {
            buf.put_slice(v);
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let checksum = hasher.finalize();

        writer.write_u32(checksum).await?;
        writer.write_all(&buf).await?;

        // Durability: Ensure data hits the disk
        writer.flush().await?;
        writer.get_ref().sync_all().await?;

        Ok(())
    }

    pub async fn replay(&self) -> Result<Vec<(Bytes, Option<Bytes>)>> {
        let mut file = File::open(&self.path).await?;
        let mut results = Vec::new();

        loop {
            let mut crc_buf = [0u8; 4];
            match file.read_exact(&mut crc_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let _expected_crc = u32::from_be_bytes(crc_buf);

            let key_len = file.read_u64().await? as usize;
            let is_val = file.read_u8().await? == 1;
            let val_len = file.read_u64().await? as usize;

            let mut key_bytes = vec![0u8; key_len];
            file.read_exact(&mut key_bytes).await?;

            let mut val_bytes = None;
            if is_val {
                let mut v = vec![0u8; val_len];
                file.read_exact(&mut v).await?;
                val_bytes = Some(Bytes::from(v));
            }

            results.push((Bytes::from(key_bytes), val_bytes));
        }

        Ok(results)
    }

    pub async fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock().await;
        writer.flush().await?;
        writer.get_ref().sync_all().await?;
        Ok(())
    }
}
