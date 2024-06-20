mod error;
pub use crate::error::{Error, Result};
use std::io::{ErrorKind, Read, Write};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;

// heavily inspired by
// https://github.com/tokio-rs/mio/blob/master/examples/tcp_server.rs
// but simplified a lot the writing of data part.

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
                        let (mut connection, address) = match server.accept() {
                            Ok((connection, address)) => (connection, address),
                            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                                // If we get a `WouldBlock` error we know our
                                // listener has no more incoming connections queued,
                                // so we can return to polling and wait for some
                                // more.
                                break;
                            }
                            Err(e) => {
                                // If it was any other kind of error, something went
                                // wrong and we terminate with an error.
                                Err(e)?
                            }
                        };

                        let token = Token(unique_token.0);
                        unique_token = Token(unique_token.0 + 1);

                        poll.registry().register(
                            &mut connection,
                            token,
                            Interest::READABLE.add(Interest::WRITABLE),
                        )?;
                        connections.insert(token, connection);
                    }
                }
                token => {
                    // Handle events for a connection.
                    let done = if let Some(connection) = connections.get_mut(&token) {
                        handle_connection(connection)?
                    } else {
                        false
                    };
                    if done {
                        if let Some(mut connection) = connections.remove(&token) {
                            poll.registry().deregister(&mut connection)?;
                        }
                    }
                }
            }
        }
    }
}

fn handle_connection(connection: &mut TcpStream) -> Result<bool> {
    // we only handle readable event not writable events
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
            Err(e) => Err(e)?,
        }
    }

    if bytes_read != 0 {
        match String::from_utf8_lossy(&received_data[..bytes_read])
            .clone()
            .as_ref()
        {
            x if x.to_lowercase() == "*1\r\n$4\r\nping\r\n" => {
                connection.write_all(b"+PONG\r\n")?;
            }
            x => {
                connection.write_all(x.as_bytes())?;
            }
        }
    }
    if connection_closed {
        return Ok(true);
    }

    Ok(false)
}
