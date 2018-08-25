use command::parse;
use std::error::Error;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

pub struct Server {
    addr: SocketAddr,
}

impl Server {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub fn listen_and_serve(&self) -> Result<(), Box<Error>> {
        let listener = TcpListener::bind(self.addr)?;
        info!("Server start: addr={}", self.addr);

        for stream in listener.incoming() {
            if let Err(e) = self.handle_stream(stream?) {
                error!("Failed to handle connection: err={}", e);
            }
        }

        Ok(())
    }

    fn handle_stream(&self, stream: TcpStream) -> Result<(), Box<Error>> {
        info!("Connected: peer={}", stream.peer_addr()?);

        let reader = BufReader::new(stream.try_clone()?);
        let mut writer = BufWriter::new(stream);

        for opt_line in reader.lines() {
            let line = opt_line?;
            match parse(&line) {
                Ok(command) => {
                    info!("Command: {:?}", command);
                    write!(writer, "OK: {:?}\n", command)?;
                    writer.flush()?;
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
}
