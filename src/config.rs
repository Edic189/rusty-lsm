use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct LsmConfig {
    pub dir: PathBuf,
    pub port: u16,
    pub max_levels: u32,
    pub l0_threshold: usize,
    pub ln_threshold: usize,
    pub block_cache_capacity: usize,
    pub block_size: usize,
    pub memtable_capacity: usize,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./db_data"),
            port: 8080,
            max_levels: 7,
            l0_threshold: 2,
            ln_threshold: 4,
            block_cache_capacity: 1000,
            block_size: 4096,
            memtable_capacity: 64 * 1024 * 1024,
        }
    }
}
