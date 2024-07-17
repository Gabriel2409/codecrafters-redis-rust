use std::collections::HashMap;

use crate::db::{RedisDb, ValueType};
use crate::parser::RedisValue;
use crate::stream::Stream;
use crate::{Error, Result};

/// Purpose of this enum is to convert a given redis value to
/// the appropriate command to be executed.
/// It only handles Arrays.
#[derive(Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Echo(String),
    Set(String, String, Option<u64>),
    Get(String),
    Info(String),
    /// All replconfs except for GETACK *
    ReplConf,
    /// GETACK has a special treatment as it is the only command that asks the replica to write
    /// back
    ReplConfGetAck,
    Psync,
    /// Wait for nb_replicas with a timeout is ms
    Wait(u64, u64),
    ConfigGet(String),
    Keys(String),
    Type(String),
    Xadd {
        key: String,
        stream_id: String,
        store: HashMap<String, String>,
    },
}

impl TryFrom<&RedisValue> for RedisCommand {
    type Error = Error;

    fn try_from(redis_value: &RedisValue) -> Result<Self> {
        match redis_value.clone() {
            RedisValue::Array(nb_elements, arr) => {
                let (command, args) = arr.split_first().ok_or_else(|| Error::EmptyCommand)?;

                match command {
                    RedisValue::BulkString(_, val) => {
                        // we could add check on size
                        match val.to_lowercase().as_ref() {
                            "ping" => {
                                if nb_elements != 1 {
                                    return Err(Error::InvalidRedisValue(redis_value.clone()));
                                }
                                Ok(Self::Ping)
                            }

                            "echo" => {
                                if nb_elements != 2 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    match &args[0] {
                                        RedisValue::BulkString(_, val) => {
                                            Ok(RedisCommand::Echo(val.clone()))
                                        }
                                        _ => Err(Error::InvalidRedisValue(redis_value.clone())),
                                    }
                                }
                            }
                            "set" => {
                                if nb_elements != 3 && nb_elements != 5 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    match (&args[0], &args[1]) {
                                        (
                                            RedisValue::BulkString(_, key),
                                            RedisValue::BulkString(_, value),
                                        ) => {
                                            let px = {
                                                if nb_elements == 5 {
                                                    match (&args[2], &args[3]) {
                                                        (
                                                            RedisValue::BulkString(_, px_id),
                                                            RedisValue::BulkString(_, px_ms),
                                                        ) => {
                                                            if px_id.to_lowercase() != "px" {
                                                                return Err(
                                                                    Error::InvalidRedisValue(
                                                                        redis_value.clone(),
                                                                    ),
                                                                );
                                                            }
                                                            Some(px_ms.parse()?)
                                                        }
                                                        _ => Err(Error::InvalidRedisValue(
                                                            redis_value.clone(),
                                                        ))?,
                                                    }
                                                } else {
                                                    None
                                                }
                                            };

                                            Ok(RedisCommand::Set(key.clone(), value.clone(), px))
                                        }
                                        _ => Err(Error::InvalidRedisValue(redis_value.clone())),
                                    }
                                }
                            }

                            "get" => {
                                if nb_elements != 2 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    match &args[0] {
                                        RedisValue::BulkString(_, key) => {
                                            Ok(RedisCommand::Get(key.clone()))
                                        }
                                        _ => Err(Error::InvalidRedisValue(redis_value.clone())),
                                    }
                                }
                            }
                            "info" => {
                                if nb_elements != 2 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    match &args[0] {
                                        RedisValue::BulkString(_, info_cmd) => {
                                            Ok(RedisCommand::Info(info_cmd.clone()))
                                        }
                                        _ => Err(Error::InvalidRedisValue(redis_value.clone())),
                                    }
                                }
                            }
                            "replconf" => {
                                if nb_elements != 3 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else if let ("GETACK", "*") = (
                                    args[0].inner_string()?.as_ref(),
                                    args[1].inner_string()?.as_ref(),
                                ) {
                                    // this is actually what the master sends the replica
                                    Ok(RedisCommand::ReplConfGetAck)
                                } else {
                                    Ok(RedisCommand::ReplConf)
                                }
                                // } else if let ("listening-port", port) =
                                //     (args[0].inner_string()?.as_ref(), args[1].inner_string()?)
                                // {
                                //     let replica_port: u16 = port.parse()?;
                                //     Ok(RedisCommand::ReplConfListeningPort(replica_port))
                                // } else {
                                //     Ok(RedisCommand::ReplConfCapa)
                                // }
                            }
                            "psync" => Ok(RedisCommand::Psync),
                            "wait" => {
                                if nb_elements != 3 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    match (&args[0], &args[1]) {
                                        (
                                            RedisValue::BulkString(_, nb_replica),
                                            RedisValue::BulkString(_, timeout),
                                        ) => {
                                            let nb_replica = nb_replica.parse()?;
                                            let timeout = timeout.parse()?;

                                            Ok(RedisCommand::Wait(nb_replica, timeout))
                                        }
                                        _ => Err(Error::InvalidRedisValue(redis_value.clone())),
                                    }
                                }
                            }
                            "config" => {
                                if nb_elements != 3 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    match (&args[0], &args[1]) {
                                        (
                                            RedisValue::BulkString(_, get),
                                            RedisValue::BulkString(_, val),
                                        ) => {
                                            if get.to_lowercase() != "get" {
                                                return Err(Error::InvalidRedisValue(
                                                    redis_value.clone(),
                                                ));
                                            }

                                            Ok(RedisCommand::ConfigGet(val.to_string()))
                                        }
                                        _ => Err(Error::InvalidRedisValue(redis_value.clone())),
                                    }
                                }
                            }
                            "keys" => {
                                if nb_elements != 2 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    match &args[0] {
                                        RedisValue::BulkString(_, pat) => {
                                            Ok(RedisCommand::Keys(pat.clone()))
                                        }
                                        _ => Err(Error::InvalidRedisValue(redis_value.clone())),
                                    }
                                }
                            }
                            "type" => {
                                if nb_elements != 2 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    match &args[0] {
                                        RedisValue::BulkString(_, key) => {
                                            Ok(RedisCommand::Type(key.clone()))
                                        }
                                        _ => Err(Error::InvalidRedisValue(redis_value.clone())),
                                    }
                                }
                            }

                            "xadd" => {
                                if nb_elements < 5 || nb_elements % 2 != 1 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    let args_as_strings = args
                                        .iter()
                                        .map(|el| {
                                            if let RedisValue::BulkString(_, val) = el {
                                                Ok(val.clone())
                                            } else {
                                                Err(Error::InvalidRedisValue(redis_value.clone()))
                                            }
                                        })
                                        // NOTE: transforms a vec of result into result of vec
                                        .collect::<Result<Vec<_>>>()?;

                                    let key = args_as_strings[0].clone();
                                    let stream_id = args_as_strings[1].clone();
                                    let mut store = HashMap::new();
                                    let mut i = 2;
                                    while i < nb_elements - 1 {
                                        store.insert(
                                            args_as_strings[i].clone(),
                                            args_as_strings[i + 1].clone(),
                                        );
                                        i += 2;
                                    }
                                    Ok(RedisCommand::Xadd {
                                        key,
                                        stream_id,
                                        store,
                                    })
                                }
                            }
                            _ => Err(Error::InvalidRedisValue(redis_value.clone())),
                        }
                    }
                    _ => Err(Error::InvalidRedisValue(redis_value.clone())),
                }
            }
            _ => Err(Error::InvalidRedisValue(redis_value.clone())),
        }
    }
}

impl RedisCommand {
    /// Whether the command should be forwarded to the other replicas.
    /// Only commands that write to the underlying db are concerned
    pub fn should_forward_to_replicas(&self) -> bool {
        matches!(self, Self::Set(_, _, _))
    }

    /// Executes command and returns a RedisValue on success
    pub fn execute(&self, db: &RedisDb) -> Result<RedisValue> {
        match self {
            Self::Ping => Ok(RedisValue::SimpleString("PONG".to_string())),
            Self::Echo(x) => Ok(RedisValue::SimpleString(x.clone())),
            Self::Set(key, value, px) => {
                db.set(key.clone(), ValueType::String(value.clone()), *px);
                Ok(RedisValue::SimpleString("OK".to_string()))
            }
            Self::Get(key) => {
                let val = db.get(key);
                match val {
                    Some(val) => match val {
                        ValueType::String(val) => Ok(RedisValue::SimpleString(val)),
                        _ => todo!("Implement get for other types"),
                    },

                    None => Ok(RedisValue::NullBulkString),
                }
            }
            Self::Info(x) => match x.as_str() {
                "replication" => {
                    let answer = db.info.to_string();

                    Ok(RedisValue::BulkString(answer.len(), answer))
                }
                _ => Err(Error::InvalidRedisCommand(self.clone())),
            },
            Self::ReplConf => Ok(RedisValue::SimpleString("OK".to_string())),
            Self::ReplConfGetAck => {
                let answer = format!("REPLCONF ACK {}", db.processed_bytes);

                Ok(RedisValue::array_of_bulkstrings_from(&answer))
            }
            Self::Psync => {
                let master_replid = db.info.master_replid.clone();
                Ok(RedisValue::SimpleString(format!(
                    "FULLRESYNC {} 0",
                    master_replid
                )))
            }
            Self::Wait(_, _) => {
                // Wait should not be executed in a standard way
                // It should instead modify the db state
                todo!()
            }
            Self::ConfigGet(val) => match val.as_str() {
                "dir" => Ok(RedisValue::array_of_bulkstrings_from(&format!(
                    "dir {}",
                    db.info.dir
                ))),
                "dbfilename" => Ok(RedisValue::array_of_bulkstrings_from(&format!(
                    "dbfilename {}",
                    db.info.dbfilename
                ))),
                _ => Err(Error::InvalidRedisCommand(self.clone())),
            },
            RedisCommand::Keys(pat) => {
                let keys = db.keys(pat);
                let joined_keys = keys.join(" ");
                Ok(RedisValue::array_of_bulkstrings_from(&joined_keys))
            }

            Self::Type(key) => {
                let val = db.get(key);
                match val {
                    Some(val) => match val {
                        ValueType::String(_) => Ok(RedisValue::SimpleString("string".to_string())),
                        ValueType::Stream(_) => Ok(RedisValue::SimpleString("stream".to_string())),
                    },

                    None => Ok(RedisValue::SimpleString("none".to_string())),
                }
            }

            Self::Xadd {
                key,
                stream_id,
                store,
            } => {
                let stream_id = db.xadd(key, stream_id, store.clone())?;
                Ok(RedisValue::bulkstring_from(&stream_id))
            }
        }
    }
}
