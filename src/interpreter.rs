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
                            if nb_elements != 3 {
                                Err(Error::InvalidRedisValue(redis_value))
                            } else {
                                match (&args[0], &args[1]) {
                                    (
                                        RedisValue::BulkString(_, key),
                                        RedisValue::BulkString(_, value),
                                    ) => {
                                        db.set(key.clone(), value.clone());

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
                                        let val = db.get(key).unwrap_or("(nil)".to_string());
                                        Ok(RedisValue::SimpleString(val))
                                    }
                                    _ => Err(Error::InvalidRedisValue(redis_value)),
                                }
                            }
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
