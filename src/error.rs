use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LsmError {
    #[error("I/O Error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization Error: {0}")]
    Serialization(String),

    #[error("Data Corruption: CRC match failed. Expected {expected}, found {found}")]
    Corruption { expected: u32, found: u32 },

    #[error("Engine is stopping, rejecting new requests")]
    EngineStopping,
}

pub type Result<T> = std::result::Result<T, LsmError>;
