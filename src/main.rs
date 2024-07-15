mod command;
mod connection_data;
mod connection_handler;
mod db;
mod error;
mod parser;
mod rdb;
mod replica;
mod token;

use crate::db::{ConnectionState, DbInfo, RedisDb};
pub use crate::error::{Error, Result};
use crate::parser::RedisValue;
use crate::token::{FIRST_UNIQUE_TOKEN, MASTER, SERVER};

use connection_handler::handle_connection;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;
use std::io::{ErrorKind, Write};
use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};

use clap::Parser;

#[derive(Parser)]
#[command(version, about="Custom redis", long_about=None )]
struct Cli {
    #[arg(long, default_value_t = 6379)]
    port: u16,
    #[arg(long)]
    replicaof: Option<String>,
    #[arg(long, default_value_t = String::from("/tmp/redis-files"))]
    dir: String,
    #[arg(long, default_value_t = String::from("dump.rdb"))]
    dbfilename: String,
}

// heavily inspired by
// https://github.com/tokio-rs/mio/blob/master/examples/tcp_server.rs
// but simplified a lot the writing of data part.

fn main() -> Result<()> {
    let args = Cli::parse();

    let mut role = "master".to_string();

    // For replicas, we save the connection stream to master
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

    // Creates the redis db
    let db_info = DbInfo::build(&role, args.port, &args.dir, &args.dbfilename);
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
        // Start of the handshake process
        db.send_ping_to_master(master_stream)?;
    }

    // tracks client calling wait. Note that we can only handle one wait.
    // TODO: improve WAIT flow
    let mut waiting_token = None;

    loop {
        // Poll Mio for events, blocking until we get an event or for 50 ms.
        poll.poll(&mut events, Some(Duration::from_millis(50)))?;

        // Process each event.
        for event in events.iter() {
            match event.token() {
                SERVER => {
                    // If this is an event for the server, it means a connection is ready to be accepted.
                    loop {
                        let (mut connection, _address) = match server.accept() {
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

                        // We give a new token for the connection
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
                    // Handles connections coming from master. This only occurs in replicas
                    // Replica should not respond to master except for getack, which is why
                    // silent is set to true
                    let master_stream_mut = master_stream
                        .as_mut()
                        .expect("Should have a connection to master");
                    let (_, _) = handle_connection(master_stream_mut, &mut db, true)
                        .map_err(|e| dbg!(e))
                        .unwrap_or((true, false));
                }
                token => {
                    // if we are in waiting state and receive an event,
                    // if it comes from a replica, it means we received an ack so we
                    // can increase the nb of obtained replicas and mark it as up to date
                    // If it comes from a new connection, it is just ignored
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
                            db.mark_replica_as_uptodate(token);
                        }
                        continue;
                    }

                    // Handle events for a connection
                    let (done, register) = if let Some(connection) = connections.get_mut(&token) {
                        handle_connection(connection, &mut db, false)
                            .map_err(|e| dbg!(e))
                            // here we force close the connection on error
                            .unwrap_or((true, false))
                    } else {
                        (false, false)
                    };

                    // register is there to handle replica connections to master
                    if done || register {
                        // Ugly patch to handle waiting state. Note that the deregister
                        // process is not really robust
                        if let ConnectionState::Waiting(_, _, _, _) = db.state {
                            waiting_token = Some(token);
                        } else if let Some(mut connection) = connections.remove(&token) {
                            if register {
                                // Here we register the connection with the correct token so
                                // that we can differentiate connections from replicas and
                                // connections from other clients.
                                poll.registry().deregister(&mut connection)?;
                                let replica_token = db.token_track.next_replica_token();
                                poll.registry().register(
                                    &mut connection,
                                    replica_token,
                                    Interest::READABLE.add(Interest::WRITABLE),
                                )?;
                                db.register_replica(connection, replica_token);
                            } else if done {
                                poll.registry().deregister(&mut connection)?;
                            }
                        }
                    }
                }
            }
        }

        // Final check on waiting state. if we are in waiting state and we either waited
        // enough or have enough ack, we write back to the waiting connection
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

                if let Some(waiting_connection) =
                    connections.get_mut(&waiting_token.expect("Waiting token should be set"))
                {
                    waiting_connection.write_all(redis_value.to_string().as_bytes())?;
                    db.state = ConnectionState::Ready;
                }
            }
        }
    }
}
