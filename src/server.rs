use command::parse;
use command::Command;
use database::Database;
use std::error::Error;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

pub struct Server {
    addr: SocketAddr,
    database: Database,
}

impl Server {
    pub fn new(addr: SocketAddr, database: Database) -> Self {
        Self { addr, database }
    }

    pub fn listen_and_serve(&mut self) -> Result<(), Box<Error>> {
        let listener = TcpListener::bind(self.addr)?;
        info!("Server start: addr={}", self.addr);

        for stream in listener.incoming() {
            if let Err(e) = self.handle_stream(stream?) {
                error!("Failed to handle connection: err={}", e);
            }
        }

        Ok(())
    }

    fn handle_stream(&mut self, stream: TcpStream) -> Result<(), Box<Error>> {
        info!("Connected: peer={}", stream.peer_addr()?);

        let reader = BufReader::new(stream.try_clone()?);
        let mut writer = BufWriter::new(stream);

        for opt_line in reader.lines() {
            let line = opt_line?;
            match parse(&line) {
                Ok(command) => {
                    info!("Command: {:?}", command);
                    self.handle_command(&command, &mut writer)?;
                }
                Err(e) => {
                    info!("{}", e);
                    write!(writer, "ERROR: {}\n", e)?;
                    writer.flush()?;
                }
            }
        }

        Ok(())
    }

    fn handle_command(
        &mut self,
        command: &Command,
        writer: &mut BufWriter<TcpStream>,
    ) -> Result<(), Box<Error>> {
        match command {
            Command::Write { key, value } => {
                self.database.write(key, value)?;
                write!(writer, "OK\n")?;
                writer.flush()?;
                Ok(())
            }
            Command::Read { key } => {
                match self.database.read(key) {
                    Some(value) => write!(writer, "OK {}\n", value)?,
                    None => write!(writer, "NOT_FOUND\n")?,
                };
                writer.flush()?;
                Ok(())
            }
            Command::Commit => {
                write!(writer, "NOT_IMPLEMENTED\n")?;
                writer.flush()?;
                Ok(())
            }
            Command::Rollback => {
                write!(writer, "NOT_IMPLEMENTED\n")?;
                writer.flush()?;
                Ok(())
            }
        }
    }
}
