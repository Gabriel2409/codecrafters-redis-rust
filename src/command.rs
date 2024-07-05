use crate::db::RedisDb;
use crate::parser::RedisValue;
use crate::{Error, Result};

#[derive(Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Echo(String),
    Set(String, String, Option<u64>),
    Get(String),
    Info(String),
    ReplConf,
    Psync,
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
        match self {
            Self::Set(_, _, _) => true,
            _ => false,
        }
    }

    /// Executes command and returns a RedisValue on success
    pub fn execute(&self, db: &RedisDb) -> Result<RedisValue> {
        match self {
            Self::Ping => Ok(RedisValue::SimpleString("PONG".to_string())),
            Self::Echo(x) => Ok(RedisValue::SimpleString(x.clone())),
            Self::Set(key, value, px) => {
                db.set(key.clone(), value.clone(), *px);
                Ok(RedisValue::SimpleString("OK".to_string()))
            }
            Self::Get(key) => {
                let val = db.get(key);
                match val {
                    Some(val) => Ok(RedisValue::SimpleString(val)),
                    None => Ok(RedisValue::NullBulkString),
                }
            }
            Self::Info(x) => match x.as_str() {
                "replication" => {
                    let answer = db.info();

                    Ok(RedisValue::BulkString(answer.len(), answer))
                }
                _ => Err(Error::InvalidRedisCommand(self.clone())),
            },
            Self::ReplConf => Ok(RedisValue::SimpleString("OK".to_string())),
            Self::Psync => {
                let master_replid = db.master_replid();
                Ok(RedisValue::SimpleString(format!(
                    "FULLRESYNC {} 0",
                    master_replid
                )))
            }
        }
    }
}
