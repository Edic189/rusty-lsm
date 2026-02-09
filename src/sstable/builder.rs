use crate::error::Result;
use crate::memtable::MemTable;
use crate::sstable::{BlockMeta, SstEntry, SstMeta};
use bloomfilter::Bloom;
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};

pub struct SstBuilder {
    path: std::path::PathBuf,
    current_block: Vec<SstEntry>,
    block_index: Vec<BlockMeta>,
    current_block_size: usize,
    target_block_size: usize, // NOVO
    bloom: Bloom<Vec<u8>>,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

impl SstBuilder {
    // Uklonjena const BLOCK_SIZE

    pub fn new(path: impl AsRef<Path>, block_size: usize) -> Self {
        let bloom = Bloom::new_for_fp_rate(10_000, 0.01);
        Self {
            path: path.as_ref().to_path_buf(),
            current_block: Vec::new(),
            block_index: Vec::new(),
            current_block_size: 0,
            target_block_size: block_size,
            bloom,
            min_key: Vec::new(),
            max_key: Vec::new(),
        }
    }

    pub async fn build<I>(mut self, iter: I, compact_to_bottom_level: bool) -> Result<()>
    where
        I: Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .await?;

        let mut writer = BufWriter::new(file);
        let mut file_offset = 0u64;
        let mut first_entry = true;

        for (key, value) in iter {
            if compact_to_bottom_level && value.is_none() {
                continue;
            }

            self.bloom.set(&key);

            if first_entry {
                self.min_key = key.clone();
                first_entry = false;
            }
            self.max_key = key.clone();

            let entry_size = key.len() + value.as_ref().map(|v| v.len()).unwrap_or(0) + 8;
            let entry = SstEntry { key, value };

            self.current_block.push(entry);
            self.current_block_size += entry_size;

            // Koristimo target_block_size iz konfiga
            if self.current_block_size >= self.target_block_size {
                self.flush_block(&mut writer, &mut file_offset).await?;
            }
        }

        if !self.current_block.is_empty() {
            self.flush_block(&mut writer, &mut file_offset).await?;
        }

        let bloom_bytes = serde_json::to_vec(&self.bloom)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

        let meta = SstMeta {
            block_index: self.block_index,
            bloom_bytes,
            min_key: self.min_key,
            max_key: self.max_key,
        };

        let meta_bytes = rkyv::to_bytes::<_, 4096>(&meta)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

        let meta_offset = file_offset;
        writer.write_all(&meta_bytes).await?;
        writer.write_u64(meta_offset).await?;

        writer.flush().await?;
        writer.get_ref().sync_all().await?;

        Ok(())
    }

    async fn flush_block(
        &mut self,
        writer: &mut BufWriter<tokio::fs::File>,
        file_offset: &mut u64,
    ) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        let first_key = self.current_block[0].key.clone();

        let block_bytes = rkyv::to_bytes::<_, 4096>(&self.current_block)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

        let mut encoder = snap::raw::Encoder::new();
        let compressed_bytes = encoder.compress_vec(&block_bytes).map_err(|e| {
            crate::error::LsmError::Serialization(format!("Compression error: {}", e))
        })?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&compressed_bytes);
        let checksum = hasher.finalize();

        let len = (compressed_bytes.len() + 4) as u64;

        self.block_index.push(BlockMeta {
            start_key: first_key,
            offset: *file_offset,
            len,
        });

        writer.write_all(&compressed_bytes).await?;
        writer.write_u32(checksum).await?;
        *file_offset += len;

        self.current_block.clear();
        self.current_block_size = 0;

        Ok(())
    }

    pub async fn build_from_memtable(self, memtable: &MemTable) -> Result<()> {
        let iter = memtable.iter().map(|entry| {
            let key = entry.key().to_vec();
            let value = entry.value().as_ref().map(|b| b.to_vec());
            (key, value)
        });

        self.build(iter, false).await
    }
}
