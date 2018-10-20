use command::Command;
use serde_json;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use protos::wal as proto;
use protobuf::stream::CodedOutputStream;
use protobuf::Message;
use byteorder::{BigEndian, WriteBytesExt};

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
        let mut record = proto::WalRecord::new();
        match entry.command {
            Command::Write{ref key, ref value} => {
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
                panic!("BUG: this kind of command cannot be written in WAL: {:?}", entry.command);
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
