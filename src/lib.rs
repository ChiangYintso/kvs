#![deny(missing_docs)]
//! A simple key/value store.

pub use error::{KvsError, Result};
pub use kv::KvStore;

mod buffer;
mod command;
mod error;
mod kv;
