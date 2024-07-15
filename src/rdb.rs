use binrw::{binrw, BinRead, BinResult};

use crate::{Error, Result};

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct Rdb {
    header: RdbHeader,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct RdbHeader {
    // Starts with REDIS
    #[brw(magic = b"REDIS")]
    #[br(parse_with=parse_version)]
    #[bw(write_with=write_version)]
    pub redis_version: u8,
    #[brw(magic = 0xFAu8)]
    bb: AuxiliaryField,
}

#[binrw::parser(reader)]
fn parse_version() -> BinResult<u8> {
    let mut buf = vec![0u8; 4];

    reader.read_exact(&mut buf)?;

    let version_str = String::from_utf8(buf).map_err(|x| binrw::Error::AssertFail {
        pos: 0,
        message: "Invalid version".to_string(),
    })?;

    let version = version_str
        .parse::<u8>()
        .map_err(|x| binrw::Error::AssertFail {
            pos: 0,
            message: "Invalid version".to_string(),
        })?;

    Ok(version)
}

#[binrw::writer(writer)]
fn write_version(version: &u8) -> BinResult<()> {
    let version_str = format!("{:04}", version);
    writer.write_all(version_str.as_bytes())?;
    Ok(())
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct AuxiliaryField {
    key: StringEncodedField,
    value: StringEncodedField,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct StringEncodedField {
    // TODO: write
    #[br(parse_with=parse_length_encoding)]
    length: u32,
    #[br(count=length, map = |x: Vec<u8>| String::from_utf8_lossy(&x).to_string() )]
    #[bw(map = |x| x.as_bytes())]
    field: String,
}

// TODO: ENSURE it is actually correct
#[binrw::parser(reader, endian)]
fn parse_length_encoding() -> BinResult<u32> {
    let byte = u8::read_options(reader, endian, ())?;
    let val = match byte >> 6 {
        0 => (byte & 0b00111111) as u32,
        1 => {
            let first_part = (byte & 0b00111111) as u32;
            let second_part = (u8::read_options(reader, endian, ())?) as u32;
            first_part << 8 & second_part
        }
        2 => {
            let second_part = u8::read_options(reader, endian, ())?;
            second_part as u32
        }
        3 => {
            todo!()
        }
        _ => unreachable!(),
    };
    Ok(val)
}

#[cfg(test)]
mod tests {

    use binrw::BinWrite;

    use super::*;
    use std::{fs::File, io::Cursor};

    #[test]
    pub fn test_rdb() -> Result<()> {
        let mut file = File::open("test_dump.rdb")?;
        let header = RdbHeader::read(&mut file)?;
        dbg!(&header.bb);

        let mut cursor = Cursor::new(vec![]);
        header.write(&mut cursor).unwrap();
        dbg!(cursor.into_inner());

        Ok(())
    }
}
