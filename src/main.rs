mod command;
mod connection_data;
mod db;
mod error;
mod parser;
mod token;

pub use crate::error::{Error, Result};
use crate::parser::{parse_rdb_length, RedisValue};
use std::io::{ErrorKind, Write};
use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};

use command::RedisCommand;
use connection_data::ConnectionData;
use db::{ConnectionState, DbInfo, RedisDb};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use nom::Finish;
use parser::parse_redis_value;
use std::collections::HashMap;
use token::{FIRST_UNIQUE_TOKEN, MASTER, SERVER};

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
    let mut connections: HashMap<Token, TcpStream> = HashMap::new();

    // Only happens for a replica
    if let Some(master_stream) = master_stream.as_mut() {
        poll.registry()
            .register(master_stream, MASTER, Interest::READABLE)?;
        db.send_ping_to_master(master_stream)?;
    }

    loop {
        // Poll Mio for events, blocking until we get an event.
        poll.poll(&mut events, Some(Duration::from_millis(100)))?;

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

                        let token = db.token_track.next_unique_token();

                        poll.registry().register(
                            &mut connection,
                            token,
                            Interest::READABLE.add(Interest::WRITABLE),
                        )?;
                        connections.insert(token, connection);
                    }
                }
                MASTER => {
                    // Handle events for a connection.
                    // here we force close the connection on error. Probably there is a better
                    // way
                    let master_stream_mut = master_stream.as_mut().unwrap();
                    let _ = handle_master_connection(master_stream_mut, &mut db)
                        .map_err(|e| dbg!(e))
                        .unwrap_or(true);
                }
                // here we are in a replica
                // token if token.0 < FIRST_UNIQUE_TOKEN.0 => {
                // }
                token => {
                    if let ConnectionState::Waiting(
                        intitial_time,
                        timeout,
                        requested_replicas,
                        obtained_replicas,
                    ) = db.state
                    {
                        if token.0 < FIRST_UNIQUE_TOKEN.0 {
                            db.state = ConnectionState::Waiting(
                                intitial_time,
                                timeout,
                                requested_replicas,
                                obtained_replicas + 1,
                            );
                        }

                        continue;
                    }

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
                    // register is there to handle replica connections to master
                    if done || register {
                        if let Some(mut connection) = connections.remove(&token) {
                            if done {
                                poll.registry().deregister(&mut connection)?;
                                if let ConnectionState::Waiting(_, _, _, _) = db.state {
                                    db.set_waiting_connection(connection);
                                }
                            } else if register {
                                // TODO: where to register?
                                poll.registry().deregister(&mut connection)?;
                                let replica_token = db.token_track.next_replica_token();

                                poll.registry().register(
                                    &mut connection,
                                    replica_token,
                                    Interest::READABLE.add(Interest::WRITABLE),
                                )?;
                                db.set_replica_stream(connection);
                            }
                        }
                    }
                }
            }
        }

        if let ConnectionState::Waiting(
            inititial_time,
            timeout,
            requested_replicas,
            obtained_replicas,
        ) = db.state
        {
            if obtained_replicas >= requested_replicas || inititial_time + timeout <= Instant::now()
            {
                let redis_value = RedisValue::Integer(obtained_replicas as i64);
                if let Some(waiting_connection) = db.waiting_connection.take() {
                    waiting_connection
                        .borrow_mut()
                        .write_all(redis_value.to_string().as_bytes())?;
                    db.state = ConnectionState::Ready;
                }
            }
        }
    }
}

/// When a client connects to the server
fn handle_connection(connection: &mut TcpStream, db: &mut RedisDb) -> Result<(bool, bool)> {
    // we only handle readable event not writable events

    let connection_data = ConnectionData::receive_data(connection)?;

    // Whether we should register the replica stream or not
    let mut register = false;
    if connection_data.bytes_read != 0 {
        if let ConnectionState::Waiting(
            intitial_time,
            timeout,
            requested_replicas,
            obtained_replicas,
        ) = db.state
        {
            let obtained_replicas = obtained_replicas + 1;
            if obtained_replicas >= requested_replicas {
                db.state = ConnectionState::Ready;
                println!("{}", obtained_replicas);
                return Ok((true, false));
            }
        }

        let input = String::from_utf8_lossy(connection_data.get_received_data()).to_string();

        let (mut input, mut redis_value) = parse_redis_value(&input).finish()?;

        let redis_command = RedisCommand::try_from(&redis_value)?;

        if let RedisCommand::Wait(nb_replicas, timeout) = redis_command {
            db.state = ConnectionState::Waiting(
                Instant::now(),
                Duration::from_millis(timeout),
                nb_replicas,
                0,
            );
            let redis_value = RedisValue::array_of_bulkstrings_from("REPLCONF GETACK *");
            db.send_to_replica(redis_value)?;

            return Ok((true, false));
        }

        let response_redis_value = redis_command.execute(db)?;

        let processed_bytes = redis_value.to_string().as_bytes().len();

        connection.write_all(response_redis_value.to_string().as_bytes())?;

        db.processed_bytes += processed_bytes;

        if let RedisCommand::Psync = redis_command {
            register = true;
            let bytes = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")?;

            // Add a small delay after sending the previous command
            std::thread::sleep(Duration::from_millis(200));

            connection.write_all(format!("${}\r\n", bytes.len()).as_bytes())?;
            connection.write_all(&bytes)?;

            // NOTE: In fact, replconf getack * is a command launched by the cli,
            // it is not automatically sent by master so we must handle it after

            println!("BEFORE MASTER SEND REPLCONF GETACK *");
            let redis_value = RedisValue::array_of_bulkstrings_from("REPLCONF GETACK *");
            connection.write_all(redis_value.to_string().as_bytes())?;
        }

        // TODO: DRY
        while !input.is_empty() {
            (input, redis_value) = parse_redis_value(input).finish()?;
            let redis_command = RedisCommand::try_from(&redis_value)?;
            let response_redis_value = redis_command.execute(db)?;

            let processed_bytes = redis_value.to_string().as_bytes().len();

            connection.write_all(response_redis_value.to_string().as_bytes())?;

            db.processed_bytes += processed_bytes;
        }

        if redis_command.should_forward_to_replicas() {
            db.send_to_replica(redis_value)?;
        }
    }
    if connection_data.connection_closed {
        return Ok((true, register));
    }
    Ok((false, register))
}

/// When a connection comes from master,
/// either as part of the handshake or when it was forwared to replicas
fn handle_master_connection(connection: &mut TcpStream, db: &mut RedisDb) -> Result<bool> {
    // we only handle readable event not writable events

    let connection_data = ConnectionData::receive_data(connection)?;

    if connection_data.bytes_read != 0 {
        let input = String::from_utf8_lossy(connection_data.get_received_data()).to_string();

        // TODO: for now the RDB file is not parsed correctly
        let (mut input, mut redis_value) = parse_redis_value(&input)
            .finish()
            .unwrap_or(("", RedisValue::NullBulkString));

        match db.state {
            ConnectionState::BeforePing => match redis_value {
                RedisValue::SimpleString(x) if x == *"PONG" => {
                    let port = db.info.port;
                    let redis_value = RedisValue::array_of_bulkstrings_from(&format!(
                        "REPLCONF listening-port {}",
                        port
                    ));
                    db.state = ConnectionState::BeforeReplConf1;
                    connection.write_all(redis_value.to_string().as_bytes())?;
                }
                _ => Err(Error::InvalidAnswerDuringHandshake(redis_value.clone()))?,
            },
            ConnectionState::BeforeReplConf1 => match redis_value {
                RedisValue::SimpleString(x) if x == *"OK" => {
                    let redis_value = RedisValue::array_of_bulkstrings_from("REPLCONF capa psync2");
                    db.state = ConnectionState::BeforeReplConf2;
                    connection.write_all(redis_value.to_string().as_bytes())?;
                }
                _ => Err(Error::InvalidAnswerDuringHandshake(redis_value.clone()))?,
            },
            ConnectionState::BeforeReplConf2 => match redis_value {
                RedisValue::SimpleString(x) if x == *"OK" => {
                    let redis_value = RedisValue::array_of_bulkstrings_from("PSYNC ? -1");
                    db.state = ConnectionState::BeforePsync;
                    connection.write_all(redis_value.to_string().as_bytes())?;
                }
                _ => Err(Error::InvalidAnswerDuringHandshake(redis_value.clone()))?,
            },
            ConnectionState::BeforePsync => {
                db.state = ConnectionState::BeforeRdbFile;
            }
            ConnectionState::BeforeRdbFile => {
                // TODO: parse rdb file
                let received_data = connection_data.get_received_data();

                let position = find_crlf_position(received_data).unwrap();

                let begin = String::from_utf8_lossy(&received_data[..position + 2]).to_string();

                let (begin, length) = parse_rdb_length(&begin).finish()?;

                //TODO: check begin is empty

                // let position = 3;
                // let length = 88;

                let rbd_bytes = &received_data[position + 2..position + 2 + length as usize];

                let end_bytes = &received_data[position + 2 + length as usize..];
                let end_input = String::from_utf8_lossy(end_bytes).to_string();

                let mut input = end_input.as_str();
                // TODO: DRY with Ready
                while !input.is_empty() {
                    (input, redis_value) = parse_redis_value(input).finish()?;
                    let redis_command = RedisCommand::try_from(&redis_value)?;
                    let response_redis_value = redis_command.execute(db)?;
                    // replica does not need to answer to master so we don't write back
                    // except for the ReplConf Ack

                    let processed_bytes = redis_value.to_string().as_bytes().len();

                    if let RedisCommand::ReplConfGetAck = redis_command {
                        println!("BEFORE REPLICA ANSWER ACK");
                        connection.write_all(response_redis_value.to_string().as_bytes())?;
                    }
                    // TODO: PROBABLY a better way
                    db.processed_bytes += processed_bytes;
                }

                db.state = ConnectionState::Ready;
            }
            ConnectionState::Ready => {
                let redis_command = RedisCommand::try_from(&redis_value)?;
                let response_redis_value = redis_command.execute(db)?;
                // replica does not need to answer to master so we don't write back
                // except for the ReplConf Ack

                let processed_bytes = redis_value.to_string().as_bytes().len();
                if let RedisCommand::ReplConfGetAck = redis_command {
                    connection.write_all(response_redis_value.to_string().as_bytes())?;
                }
                // TODO: PROBABLY a better way
                db.processed_bytes += processed_bytes;

                // sometimes, we get multiple commands at once
                // so we need to handle them
                // NOTE: we could instead have a function level loop
                while !input.is_empty() {
                    (input, redis_value) = parse_redis_value(input).finish()?;
                    let redis_command = RedisCommand::try_from(&redis_value)?;

                    let processed_bytes = redis_value.to_string().as_bytes().len();
                    let response_redis_value = redis_command.execute(db)?;
                    if let RedisCommand::ReplConfGetAck = redis_command {
                        connection.write_all(response_redis_value.to_string().as_bytes())?;
                    }
                    db.processed_bytes += processed_bytes;
                }
            }
            ConnectionState::Waiting(_, _, _, _) => {
                // should not happen
                unreachable!()
            }
        }
    }
    if connection_data.connection_closed {
        return Ok(true);
    }
    Ok(false)
}
fn find_crlf_position(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|window| window == b"\r\n")
}
