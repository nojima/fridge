use command::Command;
use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use wal::{WalWriter, WalEntry, WalReader};

pub struct Database {
    map: BTreeMap<String, String>,
    wal_reader: WalReader,
    wal_writer: WalWriter,
    next_transaction_id: u64,
}

impl Database {
    pub fn open(wal_path: &Path) -> Result<Self, Box<Error>> {
        Ok(Self {
            map: BTreeMap::new(),
            wal_writer: WalWriter::open(wal_path)?,
            wal_reader: WalReader::open(wal_path)?,
            next_transaction_id: 1,
        })
    }

    pub fn recover(&mut self) -> Result<(), Box<Error>> {
        while let Some(entry) = self.wal_reader.read()? {
            match entry.command {
                Command::Write { key, value } => {
                    self.write_to_memory(&key, &value);
                }
                Command::Commit => {
                    // TODO: should implement when support atomicity
                }
                _ => {
                    panic!("BUG: should not be happen");
                }
            }
        }
        Ok(())
    }

    pub fn begin(&mut self) -> Transaction {
        let transaction_id = self.next_transaction_id;
        self.next_transaction_id += 1;

        Transaction {
            transaction_id,
            database: self,
        }
    }

    fn write_to_memory(&mut self, key: &str, value: &str) {
        self.map.insert(key.to_string(), value.to_string());
    }

    fn read_from_memory(&self, key: &str) -> Option<String> {
        self.map.get(key).map(|value| value.to_string())
    }
}

pub struct Transaction<'a> {
    transaction_id: u64,
    database: &'a mut Database,
}

impl<'a> Transaction<'a> {
    pub fn read(&mut self, key: &str) -> Option<String> {
        self.database.read_from_memory(key)
    }

    pub fn write(&mut self, key: &str, value: &str) -> Result<(), Box<Error>> {
        self.database.wal_writer.write(&WalEntry {
            transaction_id: self.transaction_id,
            command: Command::Write {
                key: key.to_string(),
                value: value.to_string(),
            },
        })?;

        self.database.write_to_memory(key, value);

        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), Box<Error>> {
        self.database.wal_writer.write(&WalEntry {
            transaction_id: self.transaction_id,
            command: Command::Commit,
        })?;
        Ok(())
    }
}
