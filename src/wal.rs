use command::Command;
use serde_json;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
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

pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    pub fn open(path: &Path) -> Result<Self, Box<Error>> {
        let file = File::open(path)?;
        Ok(WalReader {
            reader: BufReader::new(file),
        })
    }

    pub fn read(&mut self) -> Result<Option<WalEntry>, Box<Error>> {
        let mut line = String::new();
        let len = self.reader.read_line(&mut line)?;
        if len == 0 {
            Ok(None)
        } else {
            let entry: WalEntry = serde_json::from_str(&line)?;
            Ok(Some(entry))
        }
    }
}
