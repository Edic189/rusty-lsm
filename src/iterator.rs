use crate::merge::MergeIterator;

pub struct StorageIterator {
    inner: MergeIterator<Box<dyn Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>>>,
}

impl StorageIterator {
    pub fn new(iters: Vec<Box<dyn Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>>>) -> Self {
        Self {
            inner: MergeIterator::new(iters),
        }
    }
}

impl Iterator for StorageIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((key, value)) = self.inner.next() {
            if let Some(val) = value {
                return Some((key, val));
            }
        }
        None
    }
}
