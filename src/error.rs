use thiserror::Error;

use crate::parser::RedisSentence;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid redis sentence")]
    InvalidSentence(RedisSentence),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error(transparent)]
    NetAddrParseError(#[from] std::net::AddrParseError),

    // https://stackoverflow.com/questions/77970106/how-do-i-use-from-to-convert-nom-parsing-errors-into-my-thiserror-error-variant
    // To be able to use it without the nom::err, we need to call finish after parsing
    #[error("Parsing error in the input")]
    NomParseError(nom::error::Error<String>),
}

impl From<nom::error::Error<&str>> for Error {
    fn from(err: nom::error::Error<&str>) -> Self {
        Self::NomParseError(nom::error::Error {
            input: err.input.to_string(),
            code: err.code,
        })
    }
}

pub type Result<T> = core::result::Result<T, Error>;
