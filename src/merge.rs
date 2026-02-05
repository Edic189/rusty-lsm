use std::cmp::Ordering;
use std::collections::BinaryHeap;

struct HeapItem {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    iter_index: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.iter_index.cmp(&other.iter_index))
    }
}

pub struct MergeIterator<I> {
    iters: Vec<I>,
    heap: BinaryHeap<HeapItem>,
}

impl<I> MergeIterator<I>
where
    I: Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
{
    pub fn new(iters: Vec<I>) -> Self {
        let mut heap = BinaryHeap::new();
        let mut active_iters = iters;

        for (idx, iter) in active_iters.iter_mut().enumerate() {
            if let Some((key, value)) = iter.next() {
                heap.push(HeapItem {
                    key,
                    value,
                    iter_index: idx,
                });
            }
        }

        Self {
            iters: active_iters,
            heap,
        }
    }
}

impl<I> Iterator for MergeIterator<I>
where
    I: Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
{
    type Item = (Vec<u8>, Option<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.heap.pop()?;

        while let Some(top) = self.heap.peek() {
            if top.key == current.key {
                let duplicate = self.heap.pop().unwrap();
                if let Some((next_k, next_v)) = self.iters[duplicate.iter_index].next() {
                    self.heap.push(HeapItem {
                        key: next_k,
                        value: next_v,
                        iter_index: duplicate.iter_index,
                    });
                }
            } else {
                break;
            }
        }

        if let Some((next_k, next_v)) = self.iters[current.iter_index].next() {
            self.heap.push(HeapItem {
                key: next_k,
                value: next_v,
                iter_index: current.iter_index,
            });
        }

        Some((current.key, current.value))
    }
}
