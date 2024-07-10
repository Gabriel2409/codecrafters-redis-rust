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

pub fn handle_connection(connection: &mut TcpStream, db: &mut RedisDb) -> Result<(bool, bool)> {
    // we only handle readable event not writable events

    let connection_data = ConnectionData::receive_data(connection)?;

    // Whether we should register the replica stream or not
    let mut register = false;
    if connection_data.bytes_read != 0 {
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

            // let redis_value = RedisValue::array_of_bulkstrings_from("REPLCONF GETACK *");
            // connection.write_all(redis_value.to_string().as_bytes())?;
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
pub fn handle_master_connection(connection: &mut TcpStream, db: &mut RedisDb) -> Result<bool> {
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
