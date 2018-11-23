use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Read { key: String },
    Write { key: String, value: String },
    Commit,
    Rollback,
}
