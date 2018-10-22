use protobuf::error::ProtobufError;
use std::error::Error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum WalReadError {
    IncompleteRecord(IncompleteWalRecordError),
    BrokenRecord(BrokenRecordError),
    Io(io::Error),
}

impl fmt::Display for WalReadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            WalReadError::IncompleteRecord(ref err) => write!(f, "WalReadError: {}", err),
            WalReadError::BrokenRecord(ref err) => write!(f, "WalReadError: {}", err),
            WalReadError::Io(ref err) => write!(f, "WalReadError: {}", err),
        }
    }
}

impl Error for WalReadError {
    fn cause(&self) -> Option<&Error> {
        match *self {
            WalReadError::IncompleteRecord(ref err) => Some(err),
            WalReadError::BrokenRecord(ref err) => Some(err),
            WalReadError::Io(ref err) => Some(err),
        }
    }
}

impl From<IncompleteWalRecordError> for WalReadError {
    fn from(err: IncompleteWalRecordError) -> WalReadError {
        WalReadError::IncompleteRecord(err)
    }
}

impl From<ProtobufError> for WalReadError {
    fn from(err: ProtobufError) -> WalReadError {
        WalReadError::BrokenRecord(BrokenRecordError { inner: err })
    }
}

impl From<io::Error> for WalReadError {
    fn from(err: io::Error) -> WalReadError {
        WalReadError::Io(err)
    }
}

// An error for partially written WAL record.
// When this error occurrs, we should trancate WAL.
#[derive(Debug)]
pub struct IncompleteWalRecordError {}

impl fmt::Display for IncompleteWalRecordError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
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

// An error for broken protobuf.
#[derive(Debug)]
pub struct BrokenRecordError {
    inner: ProtobufError,
}

impl fmt::Display for BrokenRecordError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

impl Error for BrokenRecordError {
    fn description(&self) -> &str {
        "broken WAL record"
    }

    fn cause(&self) -> Option<&Error> {
        Some(&self.inner)
    }
}
