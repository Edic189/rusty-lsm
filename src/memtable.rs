use crate::error::LsmError;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::ops::Bound;
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

    pub fn get(&self, key: &[u8]) -> Option<Option<Bytes>> {
        let entry = self.map.get(key)?;
        Some(entry.value().clone())
    }

    pub fn delete(&self, key: Bytes) -> MemResult<()> {
        let size_diff = key.len();
        self.map.insert(key, None);
        self.size.fetch_add(size_diff, Ordering::Relaxed);
        Ok(())
    }

    /// POPRAVAK OVDJE:
    /// Uveli smo lifetime 'a.
    /// Kažemo: self živi 'a, min/max reference žive 'a, i rezultat (Iterator) živi 'a.
    pub fn scan<'a>(
        &'a self,
        min: Bound<&'a [u8]>,
        max: Bound<&'a [u8]>,
    ) -> impl Iterator<Item = (Bytes, EntryValue)> + 'a {
        self.map
            .range::<[u8], _>((min, max))
            .map(|entry| (entry.key().clone(), entry.value().clone()))
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
