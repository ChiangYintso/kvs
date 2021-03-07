use crate::buffer::{BufReaderWithPos, BufWriterWithPos};
use crate::command::Command;
use crate::engines::KvsEngine;
use crate::{KvsError, Result};
use serde_json::Deserializer;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result, KvsEngine};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
pub struct KvStore {
    path: PathBuf,
    buf_readers: HashMap<u64, BufReaderWithPos<File>>,
    buf_writer: BufWriterWithPos<File>,
    current_log_id: u64,
    index: BTreeMap<String, ValuePos>,
    /// compact logs when uncompacted > `COMPACTION_THRESHOLD`
    uncompacted: u64,
}

impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let gen_list = sorted_gen_list(&path)?;

        // initialize BufReaderWithPos and index for all the logs
        let mut buf_readers = HashMap::new();
        let mut index = BTreeMap::new();
        let mut uncompacted = 0u64;
        let buf_writer;

        for &log_id in &gen_list {
            let mut buf_reader = BufReaderWithPos::new(File::open(log_path(&path, log_id))?)?;
            uncompacted += load_log_file(log_id, &mut buf_reader, &mut index)?;
            buf_readers.insert(log_id, buf_reader);
        }
        let current_log_id = *gen_list.last().unwrap_or(&0) + 1;
        buf_writer = new_log_file(&path, current_log_id, &mut buf_readers)?;

        Ok(KvStore {
            path,
            buf_readers,
            buf_writer,
            current_log_id,
            index,
            uncompacted,
        })
    }

    /// Clears stale entries in the log.
    fn compact(&mut self) -> Result<()> {
        let compact_log_id = self.current_log_id + 1;
        self.current_log_id += 2;
        self.buf_writer = new_log_file(&self.path, self.current_log_id, &mut self.buf_readers)?;
        let mut compaction_writer =
            new_log_file(&self.path, compact_log_id, &mut self.buf_readers)?;
        let mut new_pos = 0;
        for value_pos in self.index.values_mut() {
            let reader = self
                .buf_readers
                .get_mut(&value_pos.log_id)
                .expect("Cannot find log reader");
            if reader.pos != value_pos.start {
                reader.seek(SeekFrom::Start(value_pos.start))?;
            }

            let mut entry_reader = reader.take(value_pos.len);
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
            *value_pos = (compact_log_id, new_pos..new_pos + len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;

        // remove stale log files.
        let stale_gens: Vec<u64> = self
            .buf_readers
            .keys()
            .filter(|&&gen| gen < compact_log_id)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            self.buf_readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }
        self.uncompacted = 0;
        self.buf_readers.insert(
            compact_log_id,
            BufReaderWithPos::new(File::open(log_path(&self.path, compact_log_id))?)?,
        );
        self.buf_readers.insert(
            self.current_log_id,
            BufReaderWithPos::new(File::open(log_path(&self.path, self.current_log_id))?)?,
        );
        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.buf_writer.pos;
        serde_json::to_writer(&mut self.buf_writer, &cmd)?;
        self.buf_writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .index
                .insert(key, (self.current_log_id, pos..self.buf_writer.pos).into())
            {
                self.uncompacted += old_cmd.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::UnexpectedCommandType` if the given command type unexpected.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(value_pos) = self.index.get(&key) {
            let buf_reader = self
                .buf_readers
                .get_mut(&value_pos.log_id)
                .expect("Cannot find log reader");
            buf_reader.seek(SeekFrom::Start(value_pos.start))?;
            let cmd_reader = buf_reader.take(value_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn remove(&mut self, key: String) -> Result<()> {
        if let Some(pos) = self.index.remove(&key) {
            self.uncompacted += pos.len;
            let pos = self.buf_writer.pos;
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.buf_writer, &cmd)?;
            self.buf_writer.flush()?;

            self.uncompacted += self.buf_writer.pos - pos;

            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

/// Returns sorted generation numbers in the given directory.
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn load_log_file(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, ValuePos>,
) -> Result<u64> {
    // To make sure we read from the beginning of the file.
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction.
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.insert(key, (gen, pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                // the "remove" command itself can be deleted in the next compaction.
                // so we add its length to `uncompacted`.
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

/// Create a new log file with given generation number and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_log_file(
    path: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(gen, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

struct ValuePos {
    log_id: u64,
    start: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for ValuePos {
    fn from((log_id, range): (u64, Range<u64>)) -> Self {
        ValuePos {
            log_id,
            start: range.start,
            len: range.end - range.start,
        }
    }
}
