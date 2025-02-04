use std::collections::HashMap;

use crate::db::{RedisDb, ValueType};
use crate::parser::RedisValue;
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
    Incr(String),
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
    Xrange {
        key: String,
        stream_id_start: String,
        stream_id_end: String,
    },
    Xread {
        block: Option<u64>,
        key_offset_pairs: Vec<(String, String)>,
    },
    Multi,
    Exec,
    Discard,
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
                            "incr" => {
                                if nb_elements != 2 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    match &args[0] {
                                        RedisValue::BulkString(_, key) => {
                                            Ok(RedisCommand::Incr(key.clone()))
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
                                    let args_as_strings = get_strings_from_bulkstrings(args)
                                        .map_err(|_| {
                                            Error::InvalidRedisValue(redis_value.clone())
                                        })?;

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

                            "xrange" => {
                                if nb_elements != 4 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    let args_as_strings = get_strings_from_bulkstrings(args)
                                        .map_err(|_| {
                                            Error::InvalidRedisValue(redis_value.clone())
                                        })?;

                                    let key = args_as_strings[0].clone();
                                    let stream_id_start = args_as_strings[1].clone();
                                    let stream_id_end = args_as_strings[2].clone();
                                    Ok(RedisCommand::Xrange {
                                        key,
                                        stream_id_start,
                                        stream_id_end,
                                    })
                                }
                            }

                            "xread" => {
                                if nb_elements < 4 || nb_elements % 2 != 0 {
                                    Err(Error::InvalidRedisValue(redis_value.clone()))
                                } else {
                                    let args_as_strings = get_strings_from_bulkstrings(args)
                                        .map_err(|_| {
                                            Error::InvalidRedisValue(redis_value.clone())
                                        })?;

                                    let mut i = 0;
                                    let mut block = None;
                                    // handle block
                                    if args_as_strings[0].to_lowercase() == "block" {
                                        if nb_elements < 6 {
                                            Err(Error::InvalidRedisValue(redis_value.clone()))?
                                        }
                                        block = Some(args_as_strings[1].parse::<u64>()?);
                                        i = 2;
                                    }

                                    if args_as_strings[i] != "streams" {
                                        Err(Error::InvalidRedisValue(redis_value.clone()))?
                                    }

                                    let offset = (nb_elements - 2 - i) / 2;

                                    let mut key_offset_pairs = Vec::new();

                                    i += 1;
                                    while i + offset < args_as_strings.len() {
                                        key_offset_pairs.push((
                                            args_as_strings[i].clone(),
                                            args_as_strings[i + offset].clone(),
                                        ));
                                        i += 1;
                                    }

                                    Ok(RedisCommand::Xread {
                                        block,
                                        key_offset_pairs,
                                    })
                                }
                            }

                            "multi" => {
                                if nb_elements != 1 {
                                    return Err(Error::InvalidRedisValue(redis_value.clone()));
                                }
                                Ok(Self::Multi)
                            }
                            "exec" => {
                                if nb_elements != 1 {
                                    return Err(Error::InvalidRedisValue(redis_value.clone()));
                                }
                                Ok(Self::Exec)
                            }
                            "discard" => {
                                if nb_elements != 1 {
                                    return Err(Error::InvalidRedisValue(redis_value.clone()));
                                }
                                Ok(Self::Discard)
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
    pub fn execute(&self, db: &mut RedisDb) -> Result<RedisValue> {
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
            Self::Incr(key) => match db.incr(key) {
                Ok(val) => Ok(RedisValue::Integer(val)),
                Err(_) => Ok(RedisValue::SimpleError(
                    "ERR value is not an integer or out of range".to_string(),
                )),
            },
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
                let stream_id = db.xadd(key, stream_id, store.clone());
                match stream_id {
                    Ok(stream_id) => Ok(RedisValue::bulkstring_from(&stream_id)),
                    Err(Error::InvalidStreamId{should_be_greater_than:_, got}) => match got.as_ref() {
                        "0-0" => Ok(RedisValue::SimpleError(
                            "ERR The ID specified in XADD must be greater than 0-0".to_string(),
                        )),
                        _ => Ok(RedisValue::SimpleError(
                            "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()
                        )),
                    },
                    Err(_) => Err(Error::InvalidRedisCommand(self.clone())),
                }
            }
            Self::Xrange {
                key,
                stream_id_start,
                stream_id_end,
            } => {
                let res = db.xrange(key, stream_id_start, stream_id_end)?;

                let intermediate = res
                    .iter()
                    .map(|(id, store)| {
                        (
                            RedisValue::bulkstring_from(id),
                            RedisValue::array_of_bulkstrings_from(
                                &store
                                    .iter()
                                    .map(|(k, v)| format!("{} {}", k, v))
                                    .collect::<Vec<_>>()
                                    .join(" "),
                            ),
                        )
                    })
                    .map(|(id, store)| RedisValue::Array(2, vec![id, store]))
                    .collect::<Vec<_>>();

                Ok(RedisValue::Array(intermediate.len(), intermediate))
            }
            Self::Xread {
                block: _,
                key_offset_pairs,
            } => {
                let comb = key_offset_pairs
                    .iter()
                    .map(|(key, stream_id_start)| {
                        let intermediate = db
                            .xread(key, stream_id_start)
                            .unwrap_or_default()
                            .iter()
                            .map(|(id, store)| {
                                (
                                    RedisValue::bulkstring_from(id),
                                    RedisValue::array_of_bulkstrings_from(
                                        &store
                                            .iter()
                                            .map(|(k, v)| format!("{} {}", k, v))
                                            .collect::<Vec<_>>()
                                            .join(" "),
                                    ),
                                )
                            })
                            .map(|(id, store)| RedisValue::Array(2, vec![id, store]))
                            .collect::<Vec<_>>();

                        if intermediate.is_empty() {
                            RedisValue::Array(1, vec![RedisValue::bulkstring_from(key)])
                        } else {
                            let key_and_intermediate =
                                RedisValue::Array(intermediate.len(), intermediate);
                            RedisValue::Array(
                                2,
                                vec![RedisValue::bulkstring_from(key), key_and_intermediate],
                            )
                        }
                    })
                    .collect::<Vec<_>>();

                if comb.iter().all(|el| matches!(el, RedisValue::Array(1, _))) {
                    Ok(RedisValue::NullBulkString)
                } else {
                    Ok(RedisValue::Array(comb.len(), comb))
                }
            }

            Self::Multi => {
                // multi should not be executed in a standard way
                todo!()
            }
            Self::Exec => {
                // exec should not be executed in a standard way
                todo!()
            }
            Self::Discard => {
                // discard should not be executed in a standard way
                todo!()
            }
        }
    }
}

pub fn get_strings_from_bulkstrings(args: &[RedisValue]) -> Result<Vec<String>> {
    args.iter()
        .map(|el| {
            if let RedisValue::BulkString(_, val) = el {
                Ok(val.clone())
            } else {
                Err(Error::InvalidRedisValue(el.clone()))
            }
        })
        // NOTE: transforms a vec of result into result of vec
        .collect::<Result<Vec<_>>>()
}
