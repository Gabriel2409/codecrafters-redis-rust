use thiserror::Error;

use crate::{command::RedisCommand, parser::RedisValue};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Empty command")]
    EmptyCommand,

    #[error("Invalid redis value")]
    InvalidRedisValue(RedisValue),

    #[error("Invalid redis command")]
    InvalidRedisCommand(RedisCommand),

    #[error("Redis value cant be converted to string")]
    CantConvertToString(RedisValue),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error(transparent)]
    NetAddrParseError(#[from] std::net::AddrParseError),

    #[error(transparent)]
    FromHexError(#[from] hex::FromHexError),
    // https://stackoverflow.com/questions/77970106/how-do-i-use-from-to-convert-nom-parsing-errors-into-my-thiserror-error-variant
    // To be able to use it without the nom::err, we need to call finish after parsing
    #[error("Parsing error in the input")]
    NomParseError(nom::error::Error<String>),
}

impl<I> From<nom::error::Error<I>> for Error
where
    I: ToString,
{
    fn from(err: nom::error::Error<I>) -> Self {
        Self::NomParseError(nom::error::Error {
            input: err.input.to_string(),
            code: err.code,
        })
    }
}

pub type Result<T> = core::result::Result<T, Error>;
