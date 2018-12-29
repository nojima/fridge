use crate::command::Command;
use std::error::Error;
use std::fmt;

pub fn parse(s: &str) -> Result<Command, ParseError> {
    let words: Vec<&str> = s.split(' ').collect();
    if words.len() < 1 {
        return Err(ParseError::new("Command name is missing".to_string()));
    }
    let command_name = words[0];

    match command_name {
        "read" => {
            if words.len() != 2 {
                return Err(ParseError::new(
                    "`read` takes exactly 1 argument".to_string(),
                ));
            }
            Ok(Command::Read {
                key: words[1].to_string(),
            })
        }
        "write" => {
            if words.len() != 3 {
                return Err(ParseError::new(
                    "`write` takes exactly 2 arguments".to_string(),
                ));
            }
            Ok(Command::Write {
                key: words[1].to_string(),
                value: words[2].to_string(),
            })
        }
        "commit" => {
            if words.len() != 1 {
                return Err(ParseError::new("`commit` takes no arguments".to_string()));
            }
            Ok(Command::Commit)
        }
        "rollback" => {
            if words.len() != 1 {
                return Err(ParseError::new("`rollback` takes no arguments".to_string()));
            }
            Ok(Command::Rollback)
        }
        _ => Err(ParseError::new(format!(
            "Unknown command: {}",
            command_name
        ))),
    }
}

#[derive(Debug, Clone)]
pub struct ParseError {
    message: String,
}

impl ParseError {
    fn new(message: String) -> Self {
        Self { message }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "ParseError: {}", self.message)
    }
}

impl Error for ParseError {
    fn description(&self) -> &str {
        &self.message
    }
}

#[test]
fn test_command_read() {
    match parse("read hoge") {
        Ok(Command::Read { key }) => {
            assert_eq!(key, "hoge");
        }
        _ => {
            assert!(false);
        }
    }
}

#[test]
fn test_command_write() {
    match parse("write hoge helloworld") {
        Ok(Command::Write { key, value }) => {
            assert_eq!(key, "hoge");
            assert_eq!(value, "helloworld");
        }
        _ => {
            assert!(false);
        }
    }
}

#[test]
fn test_command_commit() {
    let command = parse("commit").unwrap();
    assert_eq!(command, Command::Commit);
}

#[test]
fn test_command_rollback() {
    let command = parse("rollback").unwrap();
    assert_eq!(command, Command::Rollback);
}

#[test]
fn test_command_empty() {
    let r = parse("");
    assert!(r.is_err());
}
