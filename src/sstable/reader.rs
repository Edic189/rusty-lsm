use crate::error::{LsmError, Result};
use crate::sstable::builder::SstFileData;
use bloomfilter::Bloom;
use memmap2::MmapOptions;
use rkyv::check_archived_root;
use std::fs::File;
use std::ops::Bound;
use std::path::Path;

pub struct SstReader {
    pub path: std::path::PathBuf,
    mmap: memmap2::Mmap,
    bloom: Bloom<Vec<u8>>,
}

impl SstReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        let archived_root =
            check_archived_root::<SstFileData>(&mmap[..]).map_err(|_| LsmError::Corruption {
                expected: 0,
                found: 0,
            })?;

        let bloom_bytes = &archived_root.bloom_bytes;
        let bloom: Bloom<Vec<u8>> = serde_json::from_slice(bloom_bytes)
            .map_err(|e| LsmError::Serialization(e.to_string()))?;

        Ok(Self { path, mmap, bloom })
    }

    pub fn get(&self, search_key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.bloom.check(&search_key.to_vec()) {
            return Ok(None);
        }

        let archived_root = unsafe { rkyv::archived_root::<SstFileData>(&self.mmap[..]) };
        let archived_entries = &archived_root.entries;

        let result =
            archived_entries.binary_search_by(|entry| entry.key.as_slice().cmp(search_key));

        match result {
            Ok(index) => {
                let entry = &archived_entries[index];
                match &entry.value {
                    rkyv::option::ArchivedOption::Some(val) => Ok(Some(val.as_slice().to_vec())),
                    rkyv::option::ArchivedOption::None => Ok(None),
                }
            }
            Err(_) => Ok(None),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        let archived_root = unsafe { rkyv::archived_root::<SstFileData>(&self.mmap[..]) };
        archived_root.entries.iter().map(|entry| {
            let key = entry.key.as_slice();
            let val = match &entry.value {
                rkyv::option::ArchivedOption::Some(v) => Some(v.as_slice()),
                rkyv::option::ArchivedOption::None => None,
            };
            (key, val)
        })
    }

    /// Vraća iterator koji počinje od prvog ključa >= start_key
    /// Ograničavanje gornje granice (end_key) ostavljamo pozivatelju (MergeIteratoru ili Engineu)
    pub fn scan(&self, start: Bound<&[u8]>) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        let archived_root = unsafe { rkyv::archived_root::<SstFileData>(&self.mmap[..]) };
        let entries = &archived_root.entries;

        // Pronađi indeks gdje početi
        let start_index = match start {
            Bound::Included(key) => entries
                .binary_search_by(|e| e.key.as_slice().cmp(key))
                .unwrap_or_else(|idx| idx),
            Bound::Excluded(key) => entries
                .binary_search_by(|e| e.key.as_slice().cmp(key))
                .map(|idx| idx + 1)
                .unwrap_or_else(|idx| idx),
            Bound::Unbounded => 0,
        };

        // Vrati iterator od tog indeksa nadalje
        entries[start_index..].iter().map(|entry| {
            let key = entry.key.as_slice();
            let val = match &entry.value {
                rkyv::option::ArchivedOption::Some(v) => Some(v.as_slice()),
                rkyv::option::ArchivedOption::None => None,
            };
            (key, val)
        })
    }
}
