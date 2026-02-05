use crate::cache::BlockCache;
use crate::error::{LsmError, Result};
use crate::sstable::{SstEntry, SstMeta};
use bloomfilter::Bloom;
use memmap2::MmapOptions;
use rkyv::check_archived_root;
use rkyv::Deserialize;
use std::collections::VecDeque;
use std::fs::File;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

pub struct SstReader {
    pub path: std::path::PathBuf,
    pub id: u128,
    mmap: memmap2::Mmap,
    meta: SstMeta,
    bloom: Bloom<Vec<u8>>,
    cache: Option<Arc<BlockCache>>,
}

impl SstReader {
    pub fn open(path: impl AsRef<Path>, id: u128, cache: Option<Arc<BlockCache>>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        if mmap.len() < 8 {
            return Err(LsmError::Corruption {
                expected: 8,
                found: mmap.len() as u32,
            });
        }
        let meta_offset_bytes = &mmap[mmap.len() - 8..];
        let meta_offset = u64::from_be_bytes(meta_offset_bytes.try_into().unwrap()) as usize;

        let meta_slice = &mmap[meta_offset..mmap.len() - 8];

        let mut aligned_meta = rkyv::AlignedVec::new();
        aligned_meta.extend_from_slice(meta_slice);

        let archived_meta =
            check_archived_root::<SstMeta>(&aligned_meta).map_err(|_| LsmError::Corruption {
                expected: 0,
                found: 0,
            })?;

        let meta: SstMeta = archived_meta.deserialize(&mut rkyv::Infallible).unwrap();

        let bloom: Bloom<Vec<u8>> = serde_json::from_slice(&meta.bloom_bytes)
            .map_err(|e| LsmError::Serialization(e.to_string()))?;

        Ok(Self {
            path,
            id,
            mmap,
            meta,
            bloom,
            cache,
        })
    }

    fn read_block(&self, offset: u64, len: u64) -> Result<Arc<Vec<u8>>> {
        if let Some(cache) = &self.cache {
            if let Some(block) = cache.get(self.id, offset) {
                return Ok(block);
            }
        }

        let offset_usize = offset as usize;
        let len_usize = len as usize;

        if len_usize < 4 {
            return Err(LsmError::Corruption {
                expected: 4,
                found: len_usize as u32,
            });
        }

        let total_slice = &self.mmap[offset_usize..offset_usize + len_usize];
        let data_len = len_usize - 4;

        let compressed_data = &total_slice[..data_len];
        let checksum_bytes = &total_slice[data_len..];
        let stored_checksum = u32::from_be_bytes(checksum_bytes.try_into().unwrap());

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(compressed_data);
        let computed_checksum = hasher.finalize();

        if computed_checksum != stored_checksum {
            return Err(LsmError::Corruption {
                expected: stored_checksum,
                found: computed_checksum,
            });
        }

        let mut decoder = snap::raw::Decoder::new();
        let decompressed_vec = decoder
            .decompress_vec(compressed_data)
            .map_err(|e| LsmError::Serialization(format!("Decompression error: {}", e)))?;

        if let Some(cache) = &self.cache {
            cache.insert(self.id, offset, decompressed_vec.clone());
        }

        Ok(Arc::new(decompressed_vec))
    }

    pub fn get(&self, search_key: &[u8]) -> Result<Option<Vec<u8>>> {
        if search_key < self.meta.min_key.as_slice() || search_key > self.meta.max_key.as_slice() {
            return Ok(None);
        }

        if !self.bloom.check(&search_key.to_vec()) {
            return Ok(None);
        }

        let block_idx = match self
            .meta
            .block_index
            .binary_search_by(|b| b.start_key.as_slice().cmp(search_key))
        {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    return Ok(None);
                }
                idx - 1
            }
        };

        let block_meta = &self.meta.block_index[block_idx];
        let block_data = self.read_block(block_meta.offset, block_meta.len)?;

        let archived_block = unsafe { rkyv::archived_root::<Vec<SstEntry>>(&block_data) };

        let entry_idx =
            archived_block.binary_search_by(|entry| entry.key.as_slice().cmp(search_key));

        match entry_idx {
            Ok(idx) => {
                let entry = &archived_block[idx];
                match &entry.value {
                    rkyv::option::ArchivedOption::Some(val) => Ok(Some(val.as_slice().to_vec())),
                    rkyv::option::ArchivedOption::None => Ok(None),
                }
            }
            Err(_) => Ok(None),
        }
    }
}

pub struct SstIterator {
    reader: Arc<SstReader>,
    current_block_idx: usize,
    current_entries: VecDeque<(Vec<u8>, Option<Vec<u8>>)>,
    end_bound: Bound<Vec<u8>>,
}

impl SstIterator {
    pub fn new(reader: Arc<SstReader>, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self {
        let start_block_idx = match start {
            Bound::Included(k) | Bound::Excluded(k) => {
                match reader
                    .meta
                    .block_index
                    .binary_search_by(|b| b.start_key.as_slice().cmp(k))
                {
                    Ok(idx) => idx,
                    Err(idx) => {
                        if idx > 0 {
                            idx - 1
                        } else {
                            0
                        }
                    }
                }
            }
            Bound::Unbounded => 0,
        };

        let end_bound = match end {
            Bound::Included(b) => Bound::Included(b.to_vec()),
            Bound::Excluded(b) => Bound::Excluded(b.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut iter = Self {
            reader,
            current_block_idx: start_block_idx,
            current_entries: VecDeque::new(),
            end_bound,
        };

        if let Err(_) = iter.load_next_block() {}

        if !iter.current_entries.is_empty() {
            match start {
                Bound::Included(k) => {
                    while let Some((first_k, _)) = iter.current_entries.front() {
                        if first_k.as_slice() < k {
                            iter.current_entries.pop_front();
                        } else {
                            break;
                        }
                    }
                }
                Bound::Excluded(k) => {
                    while let Some((first_k, _)) = iter.current_entries.front() {
                        if first_k.as_slice() <= k {
                            iter.current_entries.pop_front();
                        } else {
                            break;
                        }
                    }
                }
                _ => {}
            }
        }

        iter
    }

    fn load_next_block(&mut self) -> Result<()> {
        if self.current_block_idx >= self.reader.meta.block_index.len() {
            return Ok(());
        }

        let meta = &self.reader.meta.block_index[self.current_block_idx];
        let block_data = self.reader.read_block(meta.offset, meta.len)?;

        let archived_block = unsafe { rkyv::archived_root::<Vec<SstEntry>>(&block_data) };

        for entry in archived_block.iter() {
            let k = entry.key.as_slice().to_vec();
            let v = match &entry.value {
                rkyv::option::ArchivedOption::Some(val) => Some(val.as_slice().to_vec()),
                rkyv::option::ArchivedOption::None => None,
            };
            self.current_entries.push_back((k, v));
        }

        self.current_block_idx += 1;
        Ok(())
    }
}

impl Iterator for SstIterator {
    type Item = (Vec<u8>, Option<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((key, val)) = self.current_entries.pop_front() {
                match &self.end_bound {
                    Bound::Included(end) => {
                        if &key > end {
                            return None;
                        }
                    }
                    Bound::Excluded(end) => {
                        if &key >= end {
                            return None;
                        }
                    }
                    Bound::Unbounded => {}
                }
                return Some((key, val));
            }

            if self.current_block_idx < self.reader.meta.block_index.len() {
                if let Err(_) = self.load_next_block() {
                    return None;
                }
                if self.current_entries.is_empty() {
                    return None;
                }
            } else {
                return None;
            }
        }
    }
}
