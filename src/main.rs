mod command;
mod db;
mod error;
mod parser;

pub use crate::error::{Error, Result};
use crate::parser::RedisValue;
use std::io::{ErrorKind, Read, Write};
use std::net::ToSocketAddrs;

use command::RedisCommand;
use db::{ConnectionState, DbInfo, RedisDb};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use nom::Finish;
use parser::parse_redis_value;
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

// Standard way to accept commands
const SERVER: Token = Token(0);
// When registering a replica, the associated connection is registered with this token
const REPLICA: Token = Token(1);

fn main() -> Result<()> {
    let args = Cli::parse();

    // Creates the redis db

    let mut role = "master".to_string();
    let mut master_stream = None;
    let mut state = ConnectionState::Ready;
    match args.replicaof {
        None => {}
        Some(s) => {
            role = "slave".to_string();
            state = ConnectionState::BeforePing;

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

    let mut db = RedisDb::build(db_info, state);

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
    let mut unique_token = Token(REPLICA.0 + 1);

    // Only happens for a replica
    if let Some(master_stream) = master_stream.as_mut() {
        poll.registry()
            .register(master_stream, REPLICA, Interest::READABLE)?;
        db.send_ping_to_master(master_stream)?;
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
                REPLICA => {
                    // Handle events for a connection.
                    // here we force close the connection on error. Probably there is a better
                    // way
                    let a = master_stream.as_mut().unwrap();
                    handle_connection(a, &mut db)
                        .map_err(|e| dbg!(e))
                        .unwrap_or((true, false));
                }
                token => {
                    // Handle events for a connection.
                    let (done, register) = if let Some(connection) = connections.get_mut(&token) {
                        // here we force close the connection on error. Probably there is a better
                        // way
                        handle_connection(connection, &mut db)
                            .map_err(|e| dbg!(e))
                            .unwrap_or((true, false))
                    } else {
                        (false, false)
                    };
                    if done || register {
                        if let Some(mut connection) = connections.remove(&token) {
                            if done {
                                poll.registry().deregister(&mut connection)?;
                            }
                            if register {
                                db.set_replica_stream(connection);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn handle_connection(connection: &mut TcpStream, db: &mut RedisDb) -> Result<(bool, bool)> {
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

    let mut register = false;
    if bytes_read != 0 {
        let input = String::from_utf8_lossy(&received_data[..bytes_read]).to_string();

        // TODO: actually parse even the end of the handshake;
        // and saves it correctly
        let (_, redis_value) = parse_redis_value(&input)
            .finish()
            .unwrap_or(("aa", RedisValue::NullBulkString));

        match db.state {
            ConnectionState::BeforePing => match redis_value.inner_string()?.as_str() {
                "PONG" => {
                    let port = db.port();
                    let redis_value = RedisValue::array_of_bulkstrings_from(&format!(
                        "REPLCONF listening-port {}",
                        port
                    ));
                    db.state = ConnectionState::BeforeReplConf1;
                    connection.write_all(redis_value.to_string().as_bytes())?;
                }
                _ => Err(Error::InvalidAnswerDuringHandshake(redis_value.clone()))?,
            },
            ConnectionState::BeforeReplConf1 => match redis_value.inner_string()?.as_str() {
                "OK" => {
                    let redis_value = RedisValue::array_of_bulkstrings_from("REPLCONF capa psync2");
                    db.state = ConnectionState::BeforeReplConf2;
                    connection.write_all(redis_value.to_string().as_bytes())?;
                }
                _ => Err(Error::InvalidAnswerDuringHandshake(redis_value.clone()))?,
            },
            ConnectionState::BeforeReplConf2 => match redis_value.inner_string()?.as_str() {
                "OK" => {
                    let redis_value = RedisValue::array_of_bulkstrings_from("PSYNC ? -1");
                    db.state = ConnectionState::BeforePsync;
                    connection.write_all(redis_value.to_string().as_bytes())?;
                }
                _ => Err(Error::InvalidAnswerDuringHandshake(redis_value.clone()))?,
            },
            ConnectionState::BeforePsync => {
                db.state = ConnectionState::Ready;
                // println!("{}", redis_value);
            }
            ConnectionState::Ready => {
                let redis_command = RedisCommand::try_from(&redis_value)?;
                let response_redis_value = redis_command.execute(db)?;
                dbg!(&response_redis_value);

                connection.write_all(response_redis_value.to_string().as_bytes())?;

                if let RedisCommand::Psync = redis_command {
                    register = true;
                    let bytes = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")?;
                    connection.write_all(format!("${}\r\n", bytes.len()).as_bytes())?;
                    connection.write_all(&bytes)?;
                    // TODO: Find a way to correctly register the stream
                    // db.set_replica_stream(connection);
                }
                if redis_command.should_forward_to_replicas() {
                    dbg!(&db.replica_stream);
                    db.send_to_replica(redis_value)?;
                }
            }
        }
    }
    if connection_closed {
        return Ok((true, register));
    }
    Ok((false, register))
}
