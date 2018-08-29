use command::Command;
use serde_json;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub transaction_id: u64,
    pub command: Command,
}

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(path: &Path) -> Result<Self, Box<Error>> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        Ok(Wal { file })
    }

    pub fn write(&mut self, entry: &WalEntry) -> Result<(), Box<Error>> {
        let mut encoded = serde_json::to_vec(entry)?;
        encoded.push(b'\n');
        self.file.write_all(&encoded)?;
        self.file.sync_data()?;
        Ok(())
    }
}
