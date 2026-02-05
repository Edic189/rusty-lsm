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
    bloom: Bloom<Vec<u8>>,
}

impl SstBuilder {
    /// Veličina bloka (4 KB)
    const BLOCK_SIZE: usize = 4 * 1024;

    pub fn new(path: impl AsRef<Path>) -> Self {
        // Procjena: 10k itema za Bloom (prilagodljivo)
        let bloom = Bloom::new_for_fp_rate(10_000, 0.01);
        Self {
            path: path.as_ref().to_path_buf(),
            current_block: Vec::new(),
            block_index: Vec::new(),
            current_block_size: 0,
            bloom,
        }
    }

    pub async fn build<I>(mut self, iter: I, _approx_items: usize) -> Result<()>
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

        for (key, value) in iter {
            self.bloom.set(&key);

            // Procjena veličine entry-a (gruba)
            let entry_size = key.len() + value.as_ref().map(|v| v.len()).unwrap_or(0) + 8;

            let entry = SstEntry { key, value };
            self.current_block.push(entry);
            self.current_block_size += entry_size;

            // Ako smo prešli limit bloka, ispiši ga na disk
            if self.current_block_size >= Self::BLOCK_SIZE {
                self.flush_block(&mut writer, &mut file_offset).await?;
            }
        }

        // Ispiši preostale podatke ako ih ima
        if !self.current_block.is_empty() {
            self.flush_block(&mut writer, &mut file_offset).await?;
        }

        // --- Zapisivanje Meta Bloka (Index + Bloom) ---
        let bloom_bytes = serde_json::to_vec(&self.bloom)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

        let meta = SstMeta {
            block_index: self.block_index,
            bloom_bytes,
        };

        let meta_bytes = rkyv::to_bytes::<_, 4096>(&meta)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

        // Zapiši Meta blok
        let meta_offset = file_offset;
        writer.write_all(&meta_bytes).await?;

        // --- Footer ---
        // Na samom kraju datoteke zapisujemo 8 bajtova koji govore GDJE počinje Meta blok
        // To nam omogućuje da prilikom otvaranja datoteke pročitamo samo zadnjih 8 bajtova
        // i odmah znamo gdje su indexi.
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

        // Serijaliziraj blok koristeći rkyv
        let block_bytes = rkyv::to_bytes::<_, 4096>(&self.current_block)
            .map_err(|e| crate::error::LsmError::Serialization(e.to_string()))?;

        let len = block_bytes.len() as u64;

        // Zapiši u index gdje ovaj blok počinje
        self.block_index.push(BlockMeta {
            start_key: first_key,
            offset: *file_offset,
            len,
        });

        // Zapiši na disk
        writer.write_all(&block_bytes).await?;
        *file_offset += len;

        // Resetiraj buffer
        self.current_block.clear();
        self.current_block_size = 0;

        Ok(())
    }

    pub async fn build_from_memtable(self, memtable: &MemTable) -> Result<()> {
        let approx_items = memtable.approximate_size() / 50;
        let iter = memtable.iter().map(|entry| {
            let key = entry.key().to_vec();
            let value = entry.value().as_ref().map(|b| b.to_vec());
            (key, value)
        });

        self.build(iter, approx_items).await
    }
}
