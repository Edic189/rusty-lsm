// src/lib.rs
pub mod engine;
pub mod error;
pub mod memtable;
pub mod sstable; // This looks for src/sstable/mod.rs
pub mod wal;

pub use error::{LsmError, Result};
