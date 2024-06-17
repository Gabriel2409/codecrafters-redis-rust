use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error(transparent)]
    NetAddrParseError(#[from] std::net::AddrParseError),
}
pub type Result<T> = core::result::Result<T, Error>;
