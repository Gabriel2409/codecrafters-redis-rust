mod command;
mod db;
mod error;
mod parser;

pub use crate::error::{Error, Result};
use std::io::{ErrorKind, Read, Write};
use std::net::ToSocketAddrs;

use command::RedisCommand;
use db::{DbInfo, RedisDb};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use nom::Finish;
use parser::{parse_redis_value, RedisValue};
use std::collections::HashMap;

use clap::Parser;

#[derive(Parser)]
#[command(version, about="Custom redis", long_about=None )]
struct Cli {
    #[arg(long, default_value_t = 6379)]
    port: u16,
    #[arg(long)]
    replicaof: Option<String>,
}

// heavily inspired by
// https://github.com/tokio-rs/mio/blob/master/examples/tcp_server.rs
// but simplified a lot the writing of data part.

// Some tokens to allow us to identify which event is for which socket.
const SERVER: Token = Token(0);

fn main() -> Result<()> {
    let args = Cli::parse();

    // Creates the redis db

    let mut role = "master".to_string();
    let mut master_stream = None;
    match args.replicaof {
        None => {}
        Some(s) => {
            role = "slave".to_string();

            let arr = s.split_whitespace().collect::<Vec<_>>();
            if arr.len() == 2 {
                let master_addr = format!("{}:{}", arr[0], arr[1])
                    .to_socket_addrs()?
                    .next()
                    .ok_or_else(|| Error::InvaldMasterAddr)?;
                master_stream = Some(TcpStream::connect(master_addr)?);
            }
        }
    }

    let db_info = DbInfo::build(&role, args.port);

    let mut db = RedisDb::build(db_info);

    db.set_master_stream(master_stream);

    // Create a poll instance.
    let mut poll = Poll::new()?;
    // Create storage for events.
    let mut events = Events::with_capacity(128);

    // Setup the server socket.
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", args.port).parse()?;

    let mut server = TcpListener::bind(addr)?;

    // Start listening for incoming connections.
    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

    // Map of `Token` -> `TcpStream`.
    let mut connections = HashMap::new();
    // Unique token for each incoming connection.
    let mut unique_token = Token(SERVER.0 + 1);

    if !db.is_master() {
        db.send_handshake_to_master()?;
    }

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
                        // here we force close the connection on error. Probably there is a better
                        // way
                        handle_connection(connection, &mut db)
                            .map_err(|e| dbg!(e))
                            .unwrap_or(true)
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

fn handle_connection(connection: &mut TcpStream, db: &mut RedisDb) -> Result<bool> {
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
        let input = String::from_utf8_lossy(&received_data[..bytes_read]).to_string();

        let (_, redis_value) = parse_redis_value(&input).finish()?;
        let redis_command = RedisCommand::try_from(&redis_value)?;
        let response_redis_value = redis_command.execute(db)?;

        connection.write_all(response_redis_value.to_string().as_bytes())?;

        if let RedisCommand::Psync = redis_command {
            let addr = connection.peer_addr().unwrap();
            let replica_stream = TcpStream::connect(addr)?;
            db.set_replica_stream(replica_stream);
            let bytes = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")?;
            connection.write_all(format!("${}\r\n", bytes.len()).as_bytes())?;
            connection.write_all(&bytes)?;
        }

        // if redis_command.should_forward_to_replicas() {
        //     db.send_to_replica(redis_value)?;
        // }
    }
    if connection_closed {
        return Ok(true);
    }

    Ok(false)
}
