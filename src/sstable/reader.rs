use crate::error::{LsmError, Result};
use crate::sstable::{SstEntry, SstMeta};
use bloomfilter::Bloom;
use memmap2::MmapOptions;
use rkyv::check_archived_root;
use rkyv::Deserialize;
use std::fs::File;
use std::ops::Bound;
use std::path::Path;

pub struct SstReader {
    pub path: std::path::PathBuf,
    mmap: memmap2::Mmap,
    meta: SstMeta,
    bloom: Bloom<Vec<u8>>,
}

impl SstReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
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
            mmap,
            meta,
            bloom,
        })
    }

    pub fn get(&self, search_key: &[u8]) -> Result<Option<Vec<u8>>> {
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
        let offset = block_meta.offset as usize;
        let len = block_meta.len as usize;
        let block_slice = &self.mmap[offset..offset + len];

        let archived_block = unsafe { rkyv::archived_root::<Vec<SstEntry>>(block_slice) };

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

    pub fn scan<'a>(
        &'a self,
        start: Bound<&'a [u8]>,
    ) -> impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)> + 'a {
        let start_block_idx = match start {
            Bound::Included(k) | Bound::Excluded(k) => {
                match self
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

        let mmap_ref = &self.mmap;
        let blocks = &self.meta.block_index[start_block_idx..];

        blocks
            .iter()
            .flat_map(move |meta| {
                let offset = meta.offset as usize;
                let len = meta.len as usize;
                let slice = &mmap_ref[offset..offset + len];
                let archived_block = unsafe { rkyv::archived_root::<Vec<SstEntry>>(slice) };

                archived_block.iter().map(|entry| {
                    let k = entry.key.as_slice();
                    let v = match &entry.value {
                        rkyv::option::ArchivedOption::Some(val) => Some(val.as_slice()),
                        rkyv::option::ArchivedOption::None => None,
                    };
                    (k, v)
                })
            })
            .skip_while(move |(k, _)| match start {
                Bound::Included(start_k) => k < &start_k,
                Bound::Excluded(start_k) => k <= &start_k,
                Bound::Unbounded => false,
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        self.scan(Bound::Unbounded)
    }
}
