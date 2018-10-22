use command::Command;
use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use wal::error::WalReadError;
use wal::{WalEntry, WalReader, WalWriter};

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
        let mut volatile_map = BTreeMap::new();
        let mut last_commit_position = 0;

        loop {
            let (entry, position) = match self.wal_reader.read() {
                Ok(x) => x,
                Err(WalReadError::Eof) => {
                    info!("Successfully applied the WAL. Remove uncommited log entries: last_commit_position={}", last_commit_position);
                    self.wal_reader.truncate(last_commit_position)?;
                    break;
                }
                Err(WalReadError::IncompleteRecord) => {
                    error!("WAL has incomplete record. Truncate the log to the last commit position: last_commit_position={}", last_commit_position);
                    self.wal_reader.truncate(last_commit_position)?;
                    break;
                }
                Err(err) => return Err(From::from(err)),
            };
            match entry.command {
                Command::Write { key, value } => {
                    volatile_map.insert(key.to_string(), value.to_string());
                }
                Command::Commit => {
                    for (key, value) in volatile_map.iter() {
                        self.write_to_memory(&key, &value);
                    }
                    volatile_map.clear();
                    last_commit_position = position
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
            volatile_map: BTreeMap::new(),
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
    volatile_map: BTreeMap<String, String>,
}

impl<'a> Transaction<'a> {
    pub fn read(&mut self, key: &str) -> Option<String> {
        match self.volatile_map.get(key) {
            None => self.database.read_from_memory(key),
            Some(value) => Some(value.to_string()),
        }
    }

    pub fn write(&mut self, key: &str, value: &str) -> Result<(), Box<Error>> {
        self.volatile_map.insert(key.to_string(), value.to_string());
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), Box<Error>> {
        // TODO: WALを書いている途中にエラーになったらどうするべきか考える
        for (key, value) in self.volatile_map.iter() {
            self.database.wal_writer.write(&WalEntry {
                transaction_id: self.transaction_id,
                command: Command::Write {
                    key: key.to_string(),
                    value: value.to_string(),
                },
            })?;
        }
        self.database.wal_writer.write(&WalEntry {
            transaction_id: self.transaction_id,
            command: Command::Commit,
        })?;

        for (key, value) in self.volatile_map.iter() {
            self.database.write_to_memory(key, value);
        }

        Ok(())
    }
}
