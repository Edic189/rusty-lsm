pub mod builder;
pub mod reader;

use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

/// Metapodaci koji stoje na kraju SST datoteke.
#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive(check_bytes)]
pub struct SstMeta {
    pub block_index: Vec<BlockMeta>, // Popis blokova: (početni_ključ -> offset)
    pub bloom_bytes: Vec<u8>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct BlockMeta {
    pub start_key: Vec<u8>, // Prvi ključ u bloku
    pub offset: u64,        // Gdje počinje u datoteci
    pub len: u64,           // Dužina bloka u bajtovima
}
