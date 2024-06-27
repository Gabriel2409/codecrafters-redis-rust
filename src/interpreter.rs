use crate::parser::RedisValue;
use crate::{Error, Result};

/// Transforms a redis value into another
pub fn interpret(redis_value: RedisValue) -> Result<RedisValue> {
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
                        _ => Err(Error::InvalidRedisValue(redis_value)),
                    }
                }
                _ => Err(Error::InvalidRedisValue(redis_value)),
            }
        }
        _ => Err(Error::InvalidRedisValue(redis_value)),
    }
}
