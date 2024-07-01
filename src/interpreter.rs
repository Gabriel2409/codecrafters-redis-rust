use crate::db::RedisDb;
use crate::parser::RedisValue;
use crate::{Error, Result};

/// Transforms a redis value into another
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
                        "replconf" => Ok(RedisValue::SimpleString("OK".to_string())),
                        _ => Err(Error::InvalidRedisValue(redis_value)),
                    }
                }
                _ => Err(Error::InvalidRedisValue(redis_value)),
            }
        }
        _ => Err(Error::InvalidRedisValue(redis_value)),
    }
}
