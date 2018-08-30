extern crate protoc_rust;

use std::error::Error;
use protoc_rust::Customize;

fn main() -> Result<(), Box<Error>> {
    protoc_rust::run(protoc_rust::Args {
        out_dir: "src/protos",
        input: &["src/wal.proto"],
        includes: &[],
        customize: Customize {
            ..Default::default()
        },
    })?;
    Ok(())
}