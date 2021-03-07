#![deny(missing_docs)]
//! A simple key/value store.

pub use engines::{KvStore, KvsEngine};
pub use error::{KvsError, Result};

mod buffer;
mod command;
mod common;
pub mod engines;
mod error;
