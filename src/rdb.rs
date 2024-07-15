use std::io::SeekFrom;

use binrw::{binrw, BinRead, BinResult, BinWrite};

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct Rdb {
    header: RdbHeader,
    /// Metadata section
    #[br(parse_with=parse_auxiliary_fields)]
    auxiliary_fields: Vec<AuxiliaryField>,

    #[br(count = 1)]
    database_sections: Vec<DatabaseSection>,
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

// region: auxiliary field
#[derive(Debug)]
#[binrw]
// important to be in little endian
#[brw(little)]
pub struct AuxiliaryField {
    #[brw(magic = 0xFAu8)]
    pub key: StringEncodedField,
    pub value: StringEncodedField,
}

#[binrw::parser(reader, endian)]
fn parse_auxiliary_fields() -> BinResult<Vec<AuxiliaryField>> {
    let mut auxiliary_fields = Vec::new();

    loop {
        let byte = u8::read_options(reader, endian, ())?;
        reader.seek(SeekFrom::Current(-1))?;
        if byte != 0xFA {
            break;
        }
        auxiliary_fields.push(AuxiliaryField::read_options(reader, endian, ())?);
    }
    Ok(auxiliary_fields)
}

// endregion: auxiliary field

// region: database section

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct DatabaseSection {
    #[brw(magic = 0xFEu8)]
    pub db_number: StringEncodedField,
}

// endregion: database section

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
            0..=2 => {
                reader.seek(SeekFrom::Current(-1))?;
                let length_encoding = LengthEncoding::read_options(reader, endian, args)?;
                let mut buf = vec![0u8; length_encoding.length as usize];
                reader.read_exact(&mut buf)?;
                field = String::from_utf8_lossy(&buf).to_string();
            }
            // special case, after this there is a number on 1, 2 or 4 bytes
            // depending on the format
            // For simplicity, we keep it as string here
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
                let length_encoding = LengthEncoding { length: len as u32 };
                length_encoding.write_options(writer, endian, args)?;
                writer.write_all(bytes)?;
            }
        }
        Ok(())
    }
}

// endregion: string encoded field

// region: length encoding

#[derive(Debug)]
pub struct LengthEncoding {
    pub length: u32,
}

impl BinRead for LengthEncoding {
    type Args<'a> = ();

    fn read_options<R: std::io::prelude::Read + std::io::prelude::Seek>(
        reader: &mut R,
        endian: binrw::Endian,
        args: Self::Args<'_>,
    ) -> BinResult<Self> {
        let byte = u8::read_options(reader, endian, args)?;
        let length = match byte >> 6 {
            0 => (byte & 0b00111111) as u32,
            1 => {
                let first_part = (byte & 0b00111111) as u32;
                let second_part = (u8::read_options(reader, endian, args)?) as u32;
                first_part << 8 & second_part
            }
            2 => {
                let second_part = u8::read_options(reader, endian, args)?;
                second_part as u32
            }
            // NOTE: if MSB is 11, it is a special case, see StringEncodedField
            x => Err(binrw::Error::AssertFail {
                pos: reader.stream_position()?,
                message: format!("Length Encoding MSB can only be 00, 01, 02. Got {}", x),
            })?,
        };

        Ok(Self { length })
    }
}

impl BinWrite for LengthEncoding {
    type Args<'a> = ();

    fn write_options<W: std::io::prelude::Write + std::io::prelude::Seek>(
        &self,
        writer: &mut W,
        endian: binrw::Endian,
        args: Self::Args<'_>,
    ) -> BinResult<()> {
        let len = self.length;
        if len < 192 {
            // length fits on the rest of the byte and we are sure first two
            // msb are 00
            u8::write_options(&(len as u8), writer, endian, args)?;
        } else if len < 256 {
            // first we write the first 2 msb: 10
            u8::write_options(&0b10000000, writer, endian, args)?;

            // then we write the actual length
            u8::write_options(&(len as u8), writer, endian, args)?;
        } else {
            // we need the two bytes
            let first_part = (len >> 8) | 0b01000000;
            let second_part = len & 0b11111111;
            // first we write the first part
            u8::write_options(&(first_part as u8), writer, endian, args)?;

            // then we write the second_part
            u8::write_options(&(second_part as u8), writer, endian, args)?;
        }
        Ok(())
    }
}

// endregion: length encoding

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;

    use binrw::BinWrite;
    use pretty_hex::PrettyHex;
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
