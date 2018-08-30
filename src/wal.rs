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

pub struct WalWriter {
    file: File,
}

impl WalWriter {
    pub fn open(path: &Path) -> Result<Self, Box<Error>> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)?;
        Ok(WalWriter { file })
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
    position: u64,
}

impl WalReader {
    pub fn open(path: &Path) -> Result<Self, Box<Error>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        Ok(WalReader {
            reader: BufReader::new(file),
            position: 0,
        })
    }

    pub fn read(&mut self) -> Result<(Option<WalEntry>, u64), Box<Error>> {
        let mut line = String::new();
        let len = self.reader.read_line(&mut line)?;
        self.position += len as u64;
        if len == 0 {
            Ok((None, self.position))
        } else {
            let entry: WalEntry = serde_json::from_str(&line)?;
            Ok((Some(entry), self.position))
        }
    }

    pub fn truncate(&mut self, length: u64) -> Result<(), Box<Error>> {
        let file = self.reader.get_mut();
        file.set_len(length)?;
        Ok(())
    }
}
