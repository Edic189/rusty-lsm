use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Pomoćna struktura za Heap. Čuva trenutni element iz jednog iteratora.
struct HeapItem {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    iter_index: usize, // Indeks iteratora iz kojeg je ovaj element došao
}

// Implementacija Ord traita potrebna za BinaryHeap.
// BinaryHeap je u Rustu "Max-Heap" (najveći element na vrhu).
// Mi želimo "Min-Heap" (najmanji ključ na vrhu), pa ćemo obrnuti usporedbu.

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
        // Obrnuti poredak: uspoređujemo 'other' sa 'self'
        // Ako su ključevi isti, preferiramo onaj s VEĆIM indeksom (noviji podatak gazi stariji u LSM-u)
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.iter_index.cmp(&other.iter_index))
    }
}

/// Iterator koji spaja više sortiranih iteratora u jedan sortirani tok.
/// Ovo rješava problem RAM-a kod kompakcije.
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

        // Inicijalno napuni heap s prvim elementom iz svakog iteratora
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
        // Uzmi najmanji element s vrha heapa
        let current = self.heap.pop()?;

        // Provjeri postoji li još istih ključeva u heapu (duplikati iz drugih datoteka)
        // U LSM-u moramo izbaciti stare verzije istog ključa.
        // Zbog naše 'Ord' implementacije i logike punjenja, onaj koji smo prvi izvadili
        // je "pobjednik" (najnoviji ili jedini). Ostale iste ključeve samo "potrošimo".
        while let Some(top) = self.heap.peek() {
            if top.key == current.key {
                // Imamo duplikat. Izvadimo ga i zamijenimo sljedećim iz njegovog iteratora
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

        // Dopuni heap sljedećim elementom iz iteratora iz kojeg je došao 'current'
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
