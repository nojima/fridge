extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

mod wal;

use std::error::Error;
use std::fs::File;

fn main() -> Result<(), Box<Error>> {
    let entry1 = wal::WalEntry::Write {
        transaction_id: 100,
        key: "hello".to_string(),
        value: "world".to_string(),
    };
    let entry2 = wal::WalEntry::Commit {
        transaction_id: 101,
    };

    let file = File::create("/tmp/wal.txt")?;

    let mut wal_writer = wal::WalWriter::new(file);
    wal_writer.write(&entry1)?;
    wal_writer.write(&entry2)?;

    Ok(())
}
