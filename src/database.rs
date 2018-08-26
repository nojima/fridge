use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use wal;

pub struct Database {
    map: BTreeMap<String, String>,
    wal: wal::Wal,
}

impl Database {
    pub fn open(wal_path: &Path) -> Result<Self, Box<Error>> {
        Ok(Self {
            map: BTreeMap::new(),
            wal: wal::Wal::open(wal_path)?,
        })
    }

    pub fn read(&mut self, key: &str) -> Option<String> {
        self.map.get(key).map(|value| value.to_string())
    }

    pub fn write(&mut self, key: &str, value: &str) -> Result<(), Box<Error>> {
        self.wal.write(&wal::WalEntry::Write {
            transaction_id: 1,
            key: key.to_string(),
            value: value.to_string(),
        })?;

        self.map.insert(key.to_string(), value.to_string());

        Ok(())
    }
}
