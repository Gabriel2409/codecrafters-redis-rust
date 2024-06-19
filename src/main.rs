mod error;
pub use crate::error::{Error, Result};
use std::io::{Read, Write};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;

// https://github.com/tokio-rs/mio/blob/master/examples/tcp_server.rs

// Some tokens to allow us to identify which event is for which socket.
const SERVER: Token = Token(0);

fn main() -> Result<()> {
    // Create a poll instance.
    let mut poll = Poll::new()?;
    // Create storage for events.
    let mut events = Events::with_capacity(128);

    // Setup the server socket.
    let addr = "127.0.0.1:6379".parse()?;

    let mut server = TcpListener::bind(addr)?;

    // Start listening for incoming connections.
    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

    // Map of `Token` -> `TcpStream`.
    let mut connections = HashMap::new();
    // Unique token for each incoming connection.
    let mut unique_token = Token(SERVER.0 + 1);

    loop {
        // Poll Mio for events, blocking until we get an event.
        poll.poll(&mut events, None)?;

        // Process each event.
        for event in events.iter() {
            match event.token() {
                SERVER => {
                    // If this is an event for the server, it means a connection is ready to be accepted.
                    loop {
                        match server.accept() {
                            Ok((mut stream, _)) => {
                                let token = Token(unique_token.0);
                                unique_token = Token(unique_token.0 + 1);

                                poll.registry().register(
                                    &mut stream,
                                    token,
                                    Interest::READABLE | Interest::WRITABLE,
                                )?;
                                connections.insert(token, stream);
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                // No more connections to accept.
                                break;
                            }
                            Err(e) => {
                                println!("Error accepting connection: {}", e);
                            }
                        }
                    }
                }
                token => {
                    // Handle events for a connection.
                    if let Some(connection) = connections.get_mut(&token) {
                        handle_connection(connection)?;
                    }
                }
            }
        }
    }
}

fn handle_connection(stream: &mut TcpStream) -> Result<()> {
    let mut buffer = [0; 512];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                // Connection was closed by the client.
                return Ok(());
            }
            Ok(n) => {
                // Echo the data back to the client.
                stream.write_all(b"+PONG\r\n")?;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No more data to read.
                break;
            }
            Err(e) => {
                println!("Error reading from connection: {}", e);
            }
        }
    }
    Ok(())
}
