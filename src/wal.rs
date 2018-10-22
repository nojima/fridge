use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use command::Command;
use crc::{crc64, Hasher64};
use protobuf::Message;
use protos::wal as proto;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
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
        let mut digest = Crc64Digest::new();

        let record_len = match self.reader.read_u32::<BigEndian>() {
            Ok(n) => n,
            Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => {
                return Ok((None, self.position));
            }
            Err(err) => {
                return Err(Box::new(err));
            }
        };
        digest.write_u32::<BigEndian>(record_len)?;
        self.position += 4;

        let mut buffer = vec![0; record_len as usize];
        match self.reader.read_exact(&mut buffer[..]) {
            Ok(_) => {}
            Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => {
                return Err(Box::new(IncompleteWalRecordError {}));
            }
            Err(err) => {
                return Err(Box::new(err));
            }
        }
        digest.write(&buffer[..])?;
        self.position += record_len as u64;

        let sum = self.reader.read_u64::<BigEndian>()?;
        self.position += 8;

        // Check sum
        if digest.sum() != sum {
            error!(
                "Invalid check sum: position={}, recorded={}, calculated={}",
                self.position,
                sum,
                digest.sum()
            );
            return Err(Box::new(IncompleteWalRecordError {}));
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
        return Ok((Some(entry), self.position));
    }

    pub fn truncate(&mut self, length: u64) -> Result<(), Box<Error>> {
        let file = self.reader.get_mut();
        file.set_len(length)?;
        Ok(())
    }
}

// An error for partially written WAL record.
// When this error occurrs, we should trancate WAL.
#[derive(Debug, Clone)]
struct IncompleteWalRecordError {}

impl fmt::Display for IncompleteWalRecordError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "incomplete WAL record")
    }
}

impl Error for IncompleteWalRecordError {
    fn description(&self) -> &str {
        "incomplete WAL record"
    }

    fn cause(&self) -> Option<&Error> {
        None
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
