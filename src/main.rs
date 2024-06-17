mod error;
pub use crate::error::{Error, Result};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    // size of the buffer is arbitrary
    let mut buf = [0; 256];

    loop {
        // calling read on stream goes to the next command
        let bytes_read = stream.read(&mut buf)?;

        // No more command on the connection
        if bytes_read == 0 {
            break;
        }

        stream.write_all(b"+PONG\r\n")?;
    }

    Ok(())
}

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_connection(stream)?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
