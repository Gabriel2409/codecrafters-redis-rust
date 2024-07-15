use binrw::{binrw, BinRead, BinResult};

use crate::{Error, Result};

#[derive(Debug, BinRead)]
#[brw(little)]
pub struct RdbHeader {
    // #[br(assert(String::from_utf8_lossy(&magic_string) == "REDIS"))]
    // #[br(count = 5)]
    // pub magic_string: Vec<u8>,
    #[br(parse_with=parse_magic_string)]
    pub magic_string: String,
}

/// Helper function to parse varint fields
#[binrw::parser(reader, endian)]
fn parse_magic_string() -> BinResult<String> {
    let mut buf = [0u8; 5];
    reader.read_exact(&mut buf)?;
    let magic_str = String::from_utf8_lossy(&buf).to_string();
    if magic_str == "REDIS" {
        Ok(magic_str)
    } else {
        Err(binrw::Error::AssertFail {
            pos: 0,
            message: "Invalid magic string".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use binrw::BinRead;
    use std::fs::File;

    use super::*;

    #[test]
    pub fn test_rdb() -> Result<()> {
        let mut file = File::open("test_dump.rdb")?;
        let header = RdbHeader::read(&mut file)?;
        dbg!(header);

        Ok(())
    }
}
