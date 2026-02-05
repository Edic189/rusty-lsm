use crate::error::LsmError;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicUsize, Ordering};

type MemResult<T> = std::result::Result<T, LsmError>;
type EntryValue = Option<Bytes>;

#[derive(Debug)]
pub struct MemTable {
    map: SkipMap<Bytes, EntryValue>,
    size: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            size: AtomicUsize::new(0),
        }
    }

    pub fn put(&self, key: Bytes, value: Bytes) -> MemResult<()> {
        let size_diff = key.len() + value.len();
        self.map.insert(key, Some(value));
        self.size.fetch_add(size_diff, Ordering::Relaxed);
        Ok(())
    }

    /// Vraća:
    /// - None: Ključ ne postoji u ovoj tablici.
    /// - Some(None): Ključ postoji, ali je označen za brisanje (Tombstone).
    /// - Some(Some(val)): Ključ postoji i ima vrijednost.
    pub fn get(&self, key: &[u8]) -> Option<Option<Bytes>> {
        let entry = self.map.get(key)?;
        // Vraćamo kopiju Option<Bytes> unutar Entry-a
        Some(entry.value().clone())
    }

    pub fn delete(&self, key: Bytes) -> MemResult<()> {
        let size_diff = key.len();
        self.map.insert(key, None);
        self.size.fetch_add(size_diff, Ordering::Relaxed);
        Ok(())
    }

    pub fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = crossbeam_skiplist::map::Entry<'_, Bytes, EntryValue>> {
        self.map.iter()
    }
}
