use crate::parser::{parse_rdb_length, RedisValue};
use crate::{Error, Result};
use std::io::Write;
use std::time::{Duration, Instant};

use crate::command::RedisCommand;
use crate::connection_data::ConnectionData;
use crate::db::{ConnectionState, RedisDb};
use crate::parser::parse_redis_value;

use mio::net::TcpStream;
use nom::Finish;
/// When a client connects to the server

pub fn handle_connection(
    connection: &mut TcpStream,
    db: &mut RedisDb,
    silent: bool,
) -> Result<(bool, bool)> {
    // we only handle readable event not writable events

    let connection_data = ConnectionData::receive_data(connection)?;

    if connection_data.bytes_read == 0 {
        return Ok((connection_data.connection_closed, false));
    }

    // Whether we should register the replica stream or not
    let mut register = false;

    let input_string;
    match db.state {
        ConnectionState::BeforeRdbFile => {
            // if we are waiting for rdb file, the input we get is not a redis value.
            // However, after the rdb, the stream can contain other redis values.
            let received_data = connection_data.get_received_data();
            let position = find_crlf_position(received_data).unwrap();
            let begin = String::from_utf8_lossy(&received_data[..position + 2]).to_string();
            let (_begin, length) = parse_rdb_length(&begin).finish()?;

            // TODO: parse rdb file
            let _rbd_bytes = &received_data[position + 2..position + 2 + length as usize];

            let end_bytes = &received_data[position + 2 + length as usize..];
            input_string = String::from_utf8_lossy(end_bytes).to_string();
            db.state = ConnectionState::Ready;
        }
        _ => {
            // For all other states, we expect to receive a standard redis value.
            input_string = String::from_utf8_lossy(connection_data.get_received_data()).to_string();
        }
    }

    let mut input = input_string.as_str();
    let mut redis_value;

    while !input.is_empty() {
        (input, redis_value) = parse_redis_value(input).finish()?;

        match db.state {
            ConnectionState::BeforeRdbFile => {
                // already handled before
                unreachable!()
            }
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
            ConnectionState::Waiting(_, _, _, _) => {
                // TODO: handle commands launched while waiting
            }
            ConnectionState::Ready => {
                let redis_command = RedisCommand::try_from(&redis_value)?;
                if let RedisCommand::Wait(nb_replicas, timeout) = redis_command {
                    db.state = ConnectionState::Waiting(
                        Instant::now(),
                        Duration::from_millis(timeout),
                        nb_replicas,
                        db.get_nb_uptodate_replicas() as u64,
                    );
                    let redis_value = RedisValue::array_of_bulkstrings_from("REPLCONF GETACK *");
                    db.send_to_replicas(redis_value, true)?;

                    return Ok((true, false));
                }

                let response_redis_value = redis_command.execute(db)?;
                let processed_bytes = redis_value.to_string().as_bytes().len();

                // For replicas, only answer master if an ack is requested
                if silent {
                    if let RedisCommand::ReplConfGetAck = redis_command {
                        connection.write_all(response_redis_value.to_string().as_bytes())?;
                    }
                } else {
                    connection.write_all(response_redis_value.to_string().as_bytes())?;
                }

                db.processed_bytes += processed_bytes;
                if let RedisCommand::Psync = redis_command {
                    register = true;
                    // TODO: use actual rdb instead
                    let bytes = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")?;

                    // Add a small delay after sending the previous command
                    std::thread::sleep(Duration::from_millis(200));

                    connection.write_all(format!("${}\r\n", bytes.len()).as_bytes())?;
                    connection.write_all(&bytes)?;

                    // NOTE: In fact, replconf getack * is a command launched by the cli,
                    // it is not automatically sent by master so we must handle it after

                    // let redis_value = RedisValue::array_of_bulkstrings_from("REPLCONF GETACK *");
                    // connection.write_all(redis_value.to_string().as_bytes())?;
                }

                if redis_command.should_forward_to_replicas() {
                    db.mark_replicas_as_outdated();
                    db.send_to_replicas(redis_value, false)?;
                }
            }
        }
    }
    Ok((false, register))
}

fn find_crlf_position(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|window| window == b"\r\n")
}
