use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use command::Command;
use protobuf::Message;
use protos::wal as proto;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::io::{BufReader, Read, Write};
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
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        Ok(WalWriter { file })
    }

    pub fn write(&mut self, entry: &WalEntry) -> Result<(), Box<Error>> {
        let mut record = proto::WalRecord::new();
        match entry.command {
            Command::Write { ref key, ref value } => {
                let mut command = proto::WriteCommand::new();
                command.set_key(key.to_string());
                command.set_value(value.to_string());
                record.set_write_command(command);
            }
            Command::Commit => {
                let mut command = proto::CommitCommand::new();
                record.set_commit_command(command);
            }
            _ => {
                panic!(
                    "BUG: this kind of command cannot be written in WAL: {:?}",
                    entry.command
                );
            }
        }

        let mut buffer = vec![];
        record.write_to_writer(&mut buffer)?;

        self.file.write_u32::<BigEndian>(buffer.len() as u32)?;
        self.file.write(&buffer[..])?;

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
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(WalReader {
            reader: BufReader::new(file),
            position: 0,
        })
    }

    pub fn read(&mut self) -> Result<(Option<WalEntry>, u64), Box<Error>> {
        let record_len = match self.reader.read_u32::<BigEndian>() {
            Ok(n) => n,
            Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => {
                return Ok((None, self.position));
            }
            Err(err) => {
                return Err(Box::new(err));
            }
        };
        self.position += 4;

        let mut buffer = vec![0; record_len as usize];
        self.reader.read_exact(&mut buffer[..])?;
        self.position += record_len as u64;

        let mut record = proto::WalRecord::new();
        record.merge_from_bytes(&buffer[..])?;

        let entry = if record.has_write_command() {
            let w = record.get_write_command();
            let command = Command::Write {
                key: w.key.to_string(),
                value: w.value.to_string(),
            };
            WalEntry {
                command,
                transaction_id: 0,
            }
        } else if record.has_commit_command() {
            WalEntry {
                command: Command::Commit,
                transaction_id: 0,
            }
        } else {
            panic!("BUG: record in WAL must be write/commit command.")
        };
        return Ok((Some(entry), self.position));
    }

    pub fn truncate(&mut self, length: u64) -> Result<(), Box<Error>> {
        let file = self.reader.get_mut();
        file.set_len(length)?;
        Ok(())
    }
}
