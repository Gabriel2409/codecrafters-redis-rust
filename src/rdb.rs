use binrw::{binrw, BinRead, BinResult};

use crate::{Error, Result};

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct RdbHeader {
    // Starts with REDIS
    #[brw(magic = b"REDIS")]
    #[br(parse_with=parse_version)]
    #[bw(write_with=write_version)]
    pub redis_version: u8,
}

#[binrw::parser(reader)]
fn parse_version() -> BinResult<u8> {
    let mut buf = vec![0u8; 4];

    reader.read_exact(&mut buf)?;

    let version_str = String::from_utf8(buf).unwrap();
    let version = version_str.parse::<u8>().unwrap();
    Ok(version)
}

#[binrw::writer(writer)]
fn write_version(version: &u8) -> BinResult<()> {
    let version_str = format!("{:04}", version);
    writer.write_all(version_str.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::fs::File;

    #[test]
    pub fn test_rdb() -> Result<()> {
        let mut file = File::open("test_dump.rdb")?;
        let header = RdbHeader::read(&mut file)?;

        Ok(())
    }
}
