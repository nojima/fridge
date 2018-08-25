use serde_json;
use std::error::Error;
use std::fs::File;
use std::io::Write;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    Write {
        transaction_id: u64,
        key: String,
        value: String,
    },
    Commit {
        transaction_id: u64,
    },
}

pub struct WalWriter {
    file: File,
}

impl WalWriter {
    pub fn new(file: File) -> Self {
        Self { file }
    }
}

impl WalWriter {
    pub fn write(&mut self, entry: &WalEntry) -> Result<(), Box<Error>> {
        let mut encoded = serde_json::to_vec(entry)?;
        encoded.push(b'\n');
        self.file.write_all(&encoded)?;
        self.file.sync_data()?;
        Ok(())
    }
}
