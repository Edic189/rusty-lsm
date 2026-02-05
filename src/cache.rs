use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

pub type Block = Arc<Vec<u8>>;

pub struct BlockCache {
    cache: Mutex<LruCache<(u128, u64), Block>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap();
        Self {
            cache: Mutex::new(LruCache::new(cap)),
        }
    }

    pub fn get(&self, sst_id: u128, offset: u64) -> Option<Block> {
        let mut guard = self.cache.lock().unwrap();
        guard.get(&(sst_id, offset)).cloned()
    }

    pub fn insert(&self, sst_id: u128, offset: u64, block: Vec<u8>) {
        let mut guard = self.cache.lock().unwrap();
        guard.put((sst_id, offset), Arc::new(block));
    }
}
