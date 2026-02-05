pub mod builder;
pub mod reader;

use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive(check_bytes)]
pub struct SstMeta {
    pub block_index: Vec<BlockMeta>,
    pub bloom_bytes: Vec<u8>,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct BlockMeta {
    pub start_key: Vec<u8>,
    pub offset: u64,
    pub len: u64,
}
