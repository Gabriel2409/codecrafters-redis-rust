use nom::{
    bytes::complete::{tag, take, take_until},
    character::complete::{self, anychar},
    sequence::terminated,
    IResult,
};

use crate::{Error, Result};

#[derive(Debug, PartialEq, Clone)]
pub enum RedisValue {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    /// Contains size and actual string
    BulkString(usize, String),
    /// (shows up as (nil))
    NullBulkString,
    /// Contains nb of elements and actual values
    Array(usize, Vec<RedisValue>),
}

impl RedisValue {
    pub fn bulkstring_from(s: &str) -> Self {
        Self::BulkString(s.len(), s.to_string())
    }
    pub fn array_of_bulkstrings_from(s: &str) -> Self {
        let redis_values = s
            .split_whitespace()
            .map(RedisValue::bulkstring_from)
            .collect::<Vec<_>>();
        Self::Array(redis_values.len(), redis_values)
    }

    pub fn inner_string(&self) -> Result<String> {
        let res = match self {
            RedisValue::SimpleString(x) => x.to_string(),
            RedisValue::SimpleError(x) => x.to_string(),
            RedisValue::Integer(x) => x.to_string(),
            RedisValue::BulkString(_, x) => x.to_string(),
            RedisValue::NullBulkString => "(nil)".to_string(),
            _ => Err(Error::CantConvertToString(self.clone()))?,
        };
        Ok(res)
    }
}

impl std::fmt::Display for RedisValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SimpleString(x) => write!(f, "+{}\r\n", x),
            Self::SimpleError(x) => write!(f, "-{}\r\n", x),
            Self::Integer(x) => write!(f, ":{}\r\n", x),
            Self::BulkString(size, x) => write!(f, "${}\r\n{}\r\n", size, x),
            Self::NullBulkString => write!(f, "$-1\r\n"),
            Self::Array(size, x) => {
                write!(f, "*{}\r\n", size)?;
                for redis_value in x {
                    write!(f, "{}", redis_value)?;
                }
                Ok(())
            }
        }
    }
}

pub fn parse_redis_value(input: &str) -> IResult<&str, RedisValue> {
    let (input, symbol) = parse_symbol(input)?;
    match symbol {
        '+' => {
            let (input, val) = parse_until_crlf(input)?;
            Ok((input, RedisValue::SimpleString(val.to_string())))
        }
        '-' => {
            let (input, val) = parse_until_crlf(input)?;
            Ok((input, RedisValue::SimpleError(val.to_string())))
        }
        ':' => {
            let (input, val) = parse_redis_int(input)?;
            Ok((input, RedisValue::Integer(val)))
        }
        '$' => {
            let (input, word_length) = parse_redis_int(input)?;

            match word_length {
                -1 => Ok((input, RedisValue::NullBulkString)),
                word_length => {
                    let word_length = word_length as usize;
                    let (input, word) = parse_bulkstring_word(input, word_length)?;
                    Ok((input, RedisValue::BulkString(word_length, word.to_string())))
                }
            }
        }
        '*' => {
            let (mut input, nb_elements) = parse_redis_int(input)?;
            let nb_elements = nb_elements as usize;
            let mut redis_values = Vec::new();
            for _ in 0..nb_elements {
                let redis_value;
                // reuse of the input from outer scope
                (input, redis_value) = parse_redis_value(input)?;
                redis_values.push(redis_value);
            }
            Ok((input, RedisValue::Array(nb_elements, redis_values)))
        }
        x => {
            dbg!(x);
            dbg!(input);
            todo!()
        }
    }
}

fn parse_symbol(input: &str) -> IResult<&str, char> {
    anychar(input)
}

fn parse_redis_int(input: &str) -> IResult<&str, i64> {
    terminated(complete::i64, parse_crlf)(input)
}

fn parse_until_crlf(input: &str) -> IResult<&str, &str> {
    terminated(take_until("\r\n"), parse_crlf)(input)
}

/// Redis separates information with \r\n
fn parse_crlf(input: &str) -> IResult<&str, &str> {
    tag("\r\n")(input)
}

fn parse_bulkstring_word(input: &str, length: usize) -> IResult<&str, &str> {
    let (input, word) = take(length)(input)?;
    let (input, _) = parse_crlf(input)?;
    Ok((input, word))
}

pub fn parse_rdb_length(input: &str) -> IResult<&str, i64> {
    let (input, _symbol) = parse_symbol(input)?;
    // TODO: check symbol is $
    let (input, length) = parse_redis_int(input)?;
    Ok((input, length))
}

#[cfg(test)]
mod tests {
    use nom::Finish;

    use super::*;
    use crate::Result;

    #[test]
    fn test_parse_redis_value_simplestring() -> Result<()> {
        let initial_input = "+bonjour\r\n";
        let input = initial_input;
        let (input, redis_value) = parse_redis_value(input).finish()?;
        assert_eq!(redis_value, RedisValue::SimpleString("bonjour".to_string()));
        assert_eq!(input, "");
        assert_eq!(initial_input, redis_value.to_string());
        Ok(())
    }

    #[test]
    fn test_parse_redis_value_simpleerror() -> Result<()> {
        let initial_input = "-terrible mistake\r\n";
        let input = initial_input;
        let (input, redis_value) = parse_redis_value(input).finish()?;
        assert_eq!(
            redis_value,
            RedisValue::SimpleError("terrible mistake".to_string())
        );
        assert_eq!(input, "");
        assert_eq!(initial_input, redis_value.to_string());
        Ok(())
    }

    #[test]
    fn test_parse_redis_value_integer() -> Result<()> {
        let initial_input = ":+65\r\n";
        let input = initial_input;
        let (input, redis_value) = parse_redis_value(input).finish()?;
        assert_eq!(redis_value, RedisValue::Integer(65));
        assert_eq!(input, "");
        assert_eq!(":65\r\n", redis_value.to_string());

        let initial_input = ":455\r\n";
        let input = initial_input;
        let (input, redis_value) = parse_redis_value(input).finish()?;
        assert_eq!(redis_value, RedisValue::Integer(455));
        assert_eq!(input, "");
        assert_eq!(initial_input, redis_value.to_string());

        let initial_input = ":-879\r\n";
        let input = initial_input;
        let (input, redis_value) = parse_redis_value(input).finish()?;
        assert_eq!(redis_value, RedisValue::Integer(-879));
        assert_eq!(input, "");
        assert_eq!(initial_input, redis_value.to_string());
        Ok(())
    }

    #[test]
    fn test_parse_redis_value_bulkstring() -> Result<()> {
        let initial_input = "$7\r\nbonjour\r\n";
        let input = initial_input;
        let (input, redis_value) = parse_redis_value(input).finish()?;
        assert_eq!(
            redis_value,
            RedisValue::BulkString(7, "bonjour".to_string())
        );
        assert_eq!(input, "");
        assert_eq!(initial_input, redis_value.to_string());
        Ok(())
    }

    #[test]
    fn test_parse_redis_value_nullbulkstring() -> Result<()> {
        let initial_input = "$-1\r\n";
        let input = initial_input;
        let (input, redis_value) = parse_redis_value(input).finish()?;
        assert_eq!(redis_value, RedisValue::NullBulkString);
        assert_eq!(input, "");
        assert_eq!(initial_input, redis_value.to_string());
        Ok(())
    }

    #[test]
    fn test_parse_redis_value_array() -> Result<()> {
        let initial_input = "*2\r\n$4\r\nEcho\r\n$7\r\nbonjour\r\n";
        let input = initial_input;
        let (input, redis_value) = parse_redis_value(input).finish()?;
        assert_eq!(
            redis_value,
            RedisValue::Array(
                2,
                vec![
                    RedisValue::BulkString(4, "Echo".to_string()),
                    RedisValue::BulkString(7, "bonjour".to_string()),
                ]
            )
        );
        assert_eq!(input, "");
        assert_eq!(initial_input, redis_value.to_string());
        Ok(())
    }
}
