pub mod error;

use self::error::WalReadError;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crate::command::Command;
use crc::{crc64, Hasher64};
use log::warn;
use protobuf::Message;
use crate::protos::wal as proto;
use serde_derive::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Read, Write};
use std::path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub transaction_id: u64,
    pub command: Command,
}

pub struct WalWriter {
    file: fs::File,
}

impl WalWriter {
    pub fn open(path: &path::Path) -> io::Result<Self> {
        let file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)?;
        Ok(WalWriter { file })
    }

    pub fn write(&mut self, entry: &WalEntry) -> io::Result<()> {
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
        let mut digest = Crc64Digest::new();

        let record_size = record.compute_size();
        buffer.write_u32::<BigEndian>(record_size)?;
        digest.write_u32::<BigEndian>(record_size)?;

        record.write_to_writer(&mut buffer)?;
        record.write_to_writer(&mut digest)?;

        buffer.write_u64::<BigEndian>(digest.sum())?;

        self.file.write(&buffer[..])?;
        self.file.sync_data()?;
        Ok(())
    }
}

pub struct WalReader {
    reader: io::BufReader<fs::File>,
    position: u64,
}

impl WalReader {
    pub fn open(path: &path::Path) -> io::Result<WalReader> {
        let file = fs::OpenOptions::new().read(true).write(true).open(path)?;
        Ok(WalReader {
            reader: io::BufReader::new(file),
            position: 0,
        })
    }

    pub fn read(&mut self) -> Result<(WalEntry, u64), WalReadError> {
        let mut digest = Crc64Digest::new();

        let record_len = self.reader.read_u32::<BigEndian>().map_err(|err| {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                WalReadError::Eof
            } else {
                WalReadError::Io(err)
            }
        })?;

        digest.write_u32::<BigEndian>(record_len)?;
        self.position += 4;

        let mut buffer = vec![0; record_len as usize];
        self.reader.read_exact(&mut buffer[..])?;
        digest.write(&buffer[..])?;
        self.position += record_len as u64;

        let sum = self.reader.read_u64::<BigEndian>()?;
        self.position += 8;

        // Check sum
        if digest.sum() != sum {
            warn!(
                "Invalid check sum: position={}, recorded={}, calculated={}",
                self.position,
                sum,
                digest.sum()
            );
            return Err(WalReadError::IncompleteRecord);
        }

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
        return Ok((entry, self.position));
    }

    pub fn truncate(&mut self, length: u64) -> io::Result<()> {
        let file = self.reader.get_mut();
        file.set_len(length)?;
        Ok(())
    }
}

// Wrap crc64::Digest in order to implement io::Write.
struct Crc64Digest {
    d: crc64::Digest,
}

impl Crc64Digest {
    fn new() -> Self {
        Crc64Digest {
            d: crc64::Digest::new(crc64::ECMA),
        }
    }

    fn sum(&self) -> u64 {
        self.d.sum64()
    }
}

impl io::Write for Crc64Digest {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.d.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
