mod error;
pub use crate::error::{Error, Result};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    stream.write_all("+PONG\r\n".as_bytes())?;
    Ok(())
}

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_connection(stream)?,
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
