use nom::{
    bytes::complete::{tag, take, take_until},
    character::complete::{self, anychar},
    multi::count,
    sequence::terminated,
    IResult,
};

#[derive(Debug, PartialEq)]
enum RedisValue {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    /// Contains size and actual string
    BulkString(usize, String),
    /// Contains nb of elements and actual values
    Array(usize, Vec<RedisValue>),
}

impl std::fmt::Display for RedisValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SimpleString(x) => write!(f, "+{}\r\n", x),
            Self::SimpleError(x) => write!(f, "-{}\r\n", x),
            Self::Integer(x) => write!(f, ":{}\r\n", x),
            Self::BulkString(size, x) => write!(f, "${}\r\n{}\r\n", size, x),
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

fn parse_redis_value(input: &str) -> IResult<&str, RedisValue> {
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
            let word_length = word_length as usize;
            let (input, word) = parse_bulkstring_word(input, word_length)?;
            Ok((input, RedisValue::BulkString(word_length, word.to_string())))
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
        _ => todo!(),
    }
}

#[derive(Debug)]
pub struct RedisSentence {
    pub nb_words: usize,
    pub words: Vec<String>,
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

/// Each redis commands start with the nb of words
fn parse_nb_words(input: &str) -> IResult<&str, usize> {
    let (input, _) = tag("*")(input)?;
    let (input, nb_words) = complete::u32(input)?;
    let (input, _) = parse_crlf(input)?;
    Ok((input, nb_words as usize))
}

fn parse_bulkstring_length(input: &str) -> IResult<&str, usize> {
    let (input, _) = tag("$")(input)?;
    let (input, word_length) = complete::u32(input)?;
    let (input, _) = parse_crlf(input)?;
    Ok((input, word_length as usize))
}

fn parse_bulkstring_word(input: &str, length: usize) -> IResult<&str, &str> {
    let (input, word) = take(length)(input)?;
    let (input, _) = parse_crlf(input)?;
    Ok((input, word))
}

fn parse_bulkstring(input: &str) -> IResult<&str, &str> {
    let (input, word_length) = parse_bulkstring_length(input)?;
    let (input, word) = parse_bulkstring_word(input, word_length)?;

    Ok((input, word))
}

pub fn parse_sentence(input: &str) -> IResult<&str, RedisSentence> {
    let (input, nb_words) = parse_nb_words(input)?;

    let (input, words) = count(parse_bulkstring, nb_words)(input)?;
    let sentence = RedisSentence {
        nb_words,
        words: words.into_iter().map(|w| w.to_owned()).collect(),
    };
    Ok((input, sentence))
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

    #[test]
    fn test_parse_statement() -> Result<()> {
        let input = "*2\r\n$4\r\nEcho\r\n$7\r\nbonjour\r\n";
        let (input, sentence) = parse_sentence(input).finish()?;
        assert_eq!(sentence.nb_words, 2);
        assert_eq!(sentence.words, vec!["Echo", "bonjour"]);
        assert_eq!(input, "");
        Ok(())
    }
}
