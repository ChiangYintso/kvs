use serde::{Deserialize, Serialize};

/// Structs representing commands
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    /// kvs set <key> <value>
    Set { key: String, value: String },
    /// kvs rm <key>
    Remove { key: String },
}

impl Command {
    pub fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    pub fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}
