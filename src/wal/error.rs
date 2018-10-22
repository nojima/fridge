use std::error::Error;
use std::fmt;

// An error for partially written WAL record.
// When this error occurrs, we should trancate WAL.
#[derive(Debug, Clone)]
pub struct IncompleteWalRecordError {}

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
