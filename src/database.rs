use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use wal;

pub struct Database {
    map: BTreeMap<String, String>,
    wal: wal::Wal,
    next_transaction_id: u64,
}

impl Database {
    pub fn open(wal_path: &Path) -> Result<Self, Box<Error>> {
        Ok(Self {
            map: BTreeMap::new(),
            wal: wal::Wal::open(wal_path)?,
            next_transaction_id: 1,
        })
    }

    pub fn begin(&mut self) -> Transaction {
        let transaction_id = self.next_transaction_id;
        self.next_transaction_id += 1;

        Transaction {
            transaction_id,
            database: self,
        }
    }
}

pub struct Transaction<'a> {
    transaction_id: u64,
    database: &'a mut Database,
}

impl<'a> Transaction<'a> {
    pub fn read(&mut self, key: &str) -> Option<String> {
        self.database.map.get(key).map(|value| value.to_string())
    }

    pub fn write(&mut self, key: &str, value: &str) -> Result<(), Box<Error>> {
        self.database.wal.write(&wal::WalEntry::Write {
            transaction_id: self.transaction_id,
            key: key.to_string(),
            value: value.to_string(),
        })?;

        self.database.map.insert(key.to_string(), value.to_string());

        Ok(())
    }
}
