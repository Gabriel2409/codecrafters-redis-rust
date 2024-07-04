use crate::db::RedisDb;
use crate::parser::RedisValue;
use crate::{Error, Result};

/// Takes a redis value as input and returns a redis value as response
/// bool also mentions if should forward to replica
pub fn interpret(redis_value: RedisValue, db: &RedisDb) -> Result<RedisValue> {
    match redis_value.clone() {
        RedisValue::Array(nb_elements, arr) => {
            let (command, args) = arr.split_first().ok_or_else(|| Error::EmptyCommand)?;

            match command {
                RedisValue::BulkString(_, val) => {
                    // we could add check on size
                    match val.to_lowercase().as_ref() {
                        "ping" => {
                            if nb_elements != 1 {
                                return Err(Error::InvalidRedisValue(redis_value));
                            }
                            Ok(RedisValue::SimpleString("PONG".to_string()))
                        }

                        "echo" => {
                            if nb_elements != 2 {
                                Err(Error::InvalidRedisValue(redis_value))
                            } else {
                                match &args[0] {
                                    RedisValue::BulkString(_, val) => {
                                        Ok(RedisValue::SimpleString(val.clone()))
                                    }
                                    _ => Err(Error::InvalidRedisValue(redis_value)),
                                }
                            }
                        }
                        "set" => {
                            if nb_elements != 3 && nb_elements != 5 {
                                Err(Error::InvalidRedisValue(redis_value))
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
                                                            return Err(Error::InvalidRedisValue(
                                                                redis_value,
                                                            ));
                                                        }
                                                        Some(px_ms.parse()?)
                                                    }
                                                    _ => {
                                                        Err(Error::InvalidRedisValue(redis_value))?
                                                    }
                                                }
                                            } else {
                                                None
                                            }
                                        };

                                        db.set(key.clone(), value.clone(), px);

                                        Ok(RedisValue::SimpleString("OK".to_string()))
                                    }
                                    _ => Err(Error::InvalidRedisValue(redis_value)),
                                }
                            }
                        }

                        "get" => {
                            if nb_elements != 2 {
                                Err(Error::InvalidRedisValue(redis_value))
                            } else {
                                match &args[0] {
                                    RedisValue::BulkString(_, key) => {
                                        let val = db.get(key);
                                        match val {
                                            Some(val) => Ok(RedisValue::SimpleString(val)),
                                            None => Ok(RedisValue::NullBulkString),
                                        }
                                    }
                                    _ => Err(Error::InvalidRedisValue(redis_value)),
                                }
                            }
                        }
                        "info" => {
                            if nb_elements != 2 {
                                Err(Error::InvalidRedisValue(redis_value))
                            } else {
                                match &args[0] {
                                    RedisValue::BulkString(_, info_cmd) => {
                                        match info_cmd.as_str() {
                                            "replication" => {
                                                let answer = db.info();

                                                Ok(RedisValue::BulkString(answer.len(), answer))
                                            }
                                            _ => Err(Error::InvalidRedisValue(redis_value)),
                                        }
                                    }
                                    _ => Err(Error::InvalidRedisValue(redis_value)),
                                }
                            }
                        }
                        "replconf" => {
                            if nb_elements != 3 {
                                Err(Error::InvalidRedisValue(redis_value))
                            } else {
                                if let ("listening-port", port) =
                                    (args[0].inner_string()?.as_ref(), args[1].inner_string()?)
                                {
                                    let replica_port: u64 = port.parse()?;
                                    db.set_replica_port(replica_port);
                                }
                                Ok(RedisValue::SimpleString("OK".to_string()))
                            }
                        }
                        "psync" => {
                            let master_replid = db.master_replid();
                            Ok(RedisValue::SimpleString(format!(
                                "FULLRESYNC {} 0",
                                master_replid
                            )))
                        }
                        _ => Err(Error::InvalidRedisValue(redis_value)),
                    }
                }
                _ => Err(Error::InvalidRedisValue(redis_value)),
            }
        }
        _ => Err(Error::InvalidRedisValue(redis_value)),
    }
}
