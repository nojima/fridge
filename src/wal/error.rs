use protobuf::error::ProtobufError;
use std::io;
use failure::Fail;

#[derive(Fail, Debug)]
pub enum WalReadError {
    #[fail(display = "WalReadError: EOF")]
    Eof,

    #[fail(display = "WalReadError: IncompleteRecord")]
    IncompleteRecord,

    #[fail(display = "WalReadError: {}", _0)]
    BrokenRecord(#[cause] BrokenRecordError),

    #[fail(display = "WalReadError: {}", _0)]
    Io(#[cause] io::Error),
}

impl From<ProtobufError> for WalReadError {
    fn from(err: ProtobufError) -> WalReadError {
        WalReadError::BrokenRecord(BrokenRecordError { inner: err })
    }
}

impl From<io::Error> for WalReadError {
    fn from(err: io::Error) -> WalReadError {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            WalReadError::IncompleteRecord
        } else {
            WalReadError::Io(err)
        }
    }
}

// An error for broken protobuf.
#[derive(Fail, Debug)]
#[fail(display = "broken WAL record")]
pub struct BrokenRecordError {
    #[cause]
    inner: ProtobufError,
}
