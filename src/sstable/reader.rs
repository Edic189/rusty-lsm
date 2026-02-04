use crate::error::{LsmError, Result};
use crate::sstable::builder::SstEntry;
use memmap2::MmapOptions;
use rkyv::check_archived_root;
use std::fs::File;
use std::path::Path;

pub struct SstReader {
    mmap: memmap2::Mmap,
}

impl SstReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        let _ =
            check_archived_root::<Vec<SstEntry>>(&mmap[..]).map_err(|_| LsmError::Corruption {
                expected: 0,
                found: 0,
            })?;

        Ok(Self { mmap })
    }

    pub fn get(&self, search_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let archived_entries = unsafe { rkyv::archived_root::<Vec<SstEntry>>(&self.mmap[..]) };

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
}
