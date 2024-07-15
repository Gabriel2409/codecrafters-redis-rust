use binrw::{binread, binrw, BinRead, BinResult, BinWrite};

use crate::{Error, Result};

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct Rdb {
    header: RdbHeader,
    #[br(count = 3)]
    auxiliary_fields: Vec<AuxiliaryField>,
}

// region: header

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

// endregion: header

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct AuxiliaryField {
    #[brw(magic = 0xFAu8)]
    pub key: StringEncodedField,
    pub value: StringEncodedField,
}

// region: string encoded field

#[derive(Debug)]
pub struct StringEncodedField {
    /// Whether the msb is 11 or not
    /// This is useful for writing so that we can know whether the value is a
    /// string or an actual integer
    pub msb_11: bool,
    /// Even if the value is an integer, it is stored as a string
    pub field: String,
}

impl BinRead for StringEncodedField {
    type Args<'a> = ();

    fn read_options<R: std::io::prelude::Read + std::io::prelude::Seek>(
        reader: &mut R,
        endian: binrw::Endian,
        args: Self::Args<'_>,
    ) -> BinResult<Self> {
        let byte = u8::read_options(reader, endian, args)?;
        let mut msb_11 = false;
        let field: String;
        match byte >> 6 {
            0 => {
                let length = (byte & 0b00111111) as usize;
                let mut buf = vec![0u8; length];
                reader.read_exact(&mut buf)?;
                field = String::from_utf8_lossy(&buf).to_string();
            }
            1 => {
                let first_part = (byte & 0b00111111) as usize;
                let second_part = (u8::read_options(reader, endian, args)?) as usize;
                let length = first_part << 8 & second_part;
                let mut buf = vec![0u8; length];
                reader.read_exact(&mut buf)?;
                field = String::from_utf8_lossy(&buf).to_string();
            }
            2 => {
                let second_part = u8::read_options(reader, endian, args)?;
                let length = second_part as usize;
                let mut buf = vec![0u8; length];
                reader.read_exact(&mut buf)?;
                field = String::from_utf8_lossy(&buf).to_string();
            }
            3 => {
                msb_11 = true;
                let format = byte & 0b00111111;
                match format {
                    0 => {
                        let mut buf = [0u8; 1];
                        reader.read_exact(&mut buf)?;
                        let val = u8::from_le_bytes(buf);
                        field = format!("{}", val);
                    }
                    1 => {
                        let mut buf = [0u8; 2];
                        reader.read_exact(&mut buf)?;
                        let val = u16::from_le_bytes(buf);
                        field = format!("{}", val);
                    }
                    2 => {
                        let mut buf = [0u8; 4];
                        reader.read_exact(&mut buf)?;
                        let val = u32::from_le_bytes(buf);
                        field = format!("{}", val);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        };

        Ok(Self { msb_11, field })
    }
}

impl BinWrite for StringEncodedField {
    type Args<'a> = ();

    fn write_options<W: std::io::prelude::Write + std::io::prelude::Seek>(
        &self,
        writer: &mut W,
        endian: binrw::Endian,
        args: Self::Args<'_>,
    ) -> BinResult<()> {
        match self.msb_11 {
            true => {
                // here we actually encoded a number as string
                let num = self
                    .field
                    .parse::<u32>()
                    .expect("field should be an encoded integer");
                if num < 256 {
                    u8::write_options(&0b11000000, writer, endian, args)?;
                    u8::write_options(&(num as u8), writer, endian, args)?;
                } else if num < 65536 {
                    u8::write_options(&0b11000001, writer, endian, args)?;
                    u16::write_options(&(num as u16), writer, endian, args)?;
                } else {
                    u8::write_options(&0b11000010, writer, endian, args)?;
                    u32::write_options(&num, writer, endian, args)?;
                }
            }
            false => {
                let bytes = self.field.as_bytes();

                let len = bytes.len();
                if len < 192 {
                    // length fits on the rest of the byte and we are sure first two
                    // msb are 00
                    u8::write_options(&(len as u8), writer, endian, args)?;

                    writer.write_all(bytes)?;
                } else if len < 256 {
                    // first we write the first 2 msb: 10
                    u8::write_options(&0b10000000, writer, endian, args)?;

                    // then we write the actual length
                    u8::write_options(&(len as u8), writer, endian, args)?;

                    writer.write_all(bytes)?;
                } else {
                    // we need the two bytes
                    let first_part = (len >> 8) | 0b01000000;
                    let second_part = len & 0b11111111;
                    // first we write the first part
                    u8::write_options(&(first_part as u8), writer, endian, args)?;

                    // then we write the second_part
                    u8::write_options(&(second_part as u8), writer, endian, args)?;

                    writer.write_all(bytes)?;
                }
            }
        }
        Ok(())
    }
}

// endregion: string encoded field

#[cfg(test)]
mod tests {

    use binrw::BinWrite;
    use pretty_hex::PrettyHex;

    use super::*;
    use std::{fs::File, io::Cursor};

    #[test]
    pub fn test_rdb() -> Result<()> {
        let mut file = File::open("test_dump.rdb")?;
        let rdb = Rdb::read(&mut file)?;
        dbg!(&rdb.auxiliary_fields);

        let mut cursor = Cursor::new(vec![]);
        rdb.write(&mut cursor).unwrap();

        println!("{}", cursor.into_inner().hex_dump());

        Ok(())
    }
}
