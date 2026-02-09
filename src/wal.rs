use crate::batch::BatchOp;
use crate::error::Result;
use bytes::{BufMut, Bytes};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};
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
            .read(true)
            .write(true)
            .open(&path)
            .await?;

        Ok(Self {
            path,
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub async fn write_batch(&self, ops: &[BatchOp]) -> Result<()> {
        let mut writer = self.writer.lock().await;

        let ops_bytes = bincode::serialize(ops)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

        let mut payload = Vec::with_capacity(8 + ops_bytes.len());
        payload.put_u64(ops_bytes.len() as u64);
        payload.put_slice(&ops_bytes);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload);
        let checksum = hasher.finalize();

        writer.write_u32(checksum).await?;
        writer.write_all(&payload).await?;

        writer.flush().await?;
        writer.get_ref().sync_all().await?;

        Ok(())
    }

    pub async fn replay(&self) -> Result<Vec<(Bytes, Option<Bytes>)>> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .await?;
        let mut results = Vec::new();
        let mut valid_pos = 0u64;

        loop {
            let mut crc_buf = [0u8; 4];
            match file.read_exact(&mut crc_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
            let expected_crc = u32::from_be_bytes(crc_buf);

            let mut len_buf = [0u8; 8];
            if let Err(_) = file.read_exact(&mut len_buf).await {
                tracing::warn!(
                    "WAL corrupted: Incomplete length header. Truncating to {}",
                    valid_pos
                );
                file.set_len(valid_pos).await?;
                break;
            }
            let data_len = u64::from_be_bytes(len_buf) as usize;

            let mut data = vec![0u8; data_len];
            if let Err(_) = file.read_exact(&mut data).await {
                tracing::warn!(
                    "WAL corrupted: Incomplete data body. Truncating to {}",
                    valid_pos
                );
                file.set_len(valid_pos).await?;
                break;
            }

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&len_buf);
            hasher.update(&data);
            let computed_crc = hasher.finalize();

            if computed_crc != expected_crc {
                tracing::warn!("WAL corrupted: CRC Mismatch. Truncating to {}", valid_pos);
                file.set_len(valid_pos).await?;
                break;
            }

            let ops: Vec<BatchOp> = bincode::deserialize(&data)
                .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

            for op in ops {
                match op {
                    BatchOp::Put(k, v) => results.push((Bytes::from(k), Some(Bytes::from(v)))),
                    BatchOp::Delete(k) => results.push((Bytes::from(k), None)),
                }
            }

            valid_pos = file.seek(SeekFrom::Current(0)).await?;
        }

        Ok(results)
    }

    pub async fn reset(&self) -> Result<()> {
        let mut writer = self.writer.lock().await;
        writer.flush().await?;
        let file = writer.get_mut();
        file.set_len(0).await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.sync_all().await?;
        Ok(())
    }
}
