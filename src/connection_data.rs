use mio::net::TcpStream;
use std::io::{ErrorKind, Read, Result};

/// Helper struct to help receiving data with mio
pub struct ConnectionData {
    pub bytes_read: usize,
    pub received_data: Vec<u8>,
    pub connection_closed: bool,
}
impl ConnectionData {
    pub fn receive_data(connection: &mut TcpStream) -> Result<ConnectionData> {
        let mut connection_closed = false;
        let mut received_data = vec![0; 512];
        let mut bytes_read = 0;
        loop {
            match connection.read(&mut received_data[bytes_read..]) {
                Ok(0) => {
                    // Reading 0 bytes means the other side has closed the
                    // connection or is done writing, then so are we.
                    connection_closed = true;
                    break;
                }
                Ok(n) => {
                    bytes_read += n;
                    if bytes_read == received_data.len() {
                        received_data.resize(received_data.len() + 512, 0);
                    }
                }
                // Would block "errors" are the OS's way of saying that the
                // connection is not actually ready to perform this I/O operation.
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                // Other errors we'll consider fatal.
                Err(e) => return Err(e),
            }
        }
        Ok(ConnectionData {
            bytes_read,
            received_data,
            connection_closed,
        })
    }

    pub fn get_received_data(&self) -> &[u8] {
        &self.received_data[..self.bytes_read]
    }
}
