use std::error::Error;
use std::io::Read;
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

    fn handle_stream(&self, mut stream: TcpStream) -> Result<(), Box<Error>> {
        info!("Connected: peer={}", stream.peer_addr()?);

        let mut buffer = String::new();
        stream.read_to_string(&mut buffer)?;

        info!("Client sent: {}", buffer);

        Ok(())
    }
}
