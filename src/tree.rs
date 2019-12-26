use crate::{Database, Disk};
use byteorder::{BigEndian, ReadBytesExt};

use std::convert::TryInto;
use std::io::SeekFrom;
use std::{
    io,
    sync::{Arc, Mutex},
};

struct TreeNode {}

enum Value {
    Number(i64),
    String(String),
}
impl Value {
    const NUMBER_TAG: u8 = 0b01;
    const STRING_TAG: u8 = 0b10;

    fn read(disk: &mut impl Disk) -> io::Result<Value> {
        let tag = disk.read_u8()?;
        let value = match tag {
            Self::NUMBER_TAG => Value::Number(disk.read_i64::<BigEndian>()?),
            Self::STRING_TAG => {
                let disk_ptr = disk.read_u64::<BigEndian>()?;
                disk.seek(SeekFrom::Start(disk_ptr))?;
                let len: usize = disk.read_u64::<BigEndian>()?.try_into().unwrap();
                let mut buf = vec![0; len];
                disk.read_exact(&mut buf)?;
                Value::String(String::from_utf8(buf).unwrap())
            }
            n => panic!("Encountered an unknown value tag: {}", n),
        };
        Ok(value)
    }
    fn store(&self, disk: &mut impl Disk) -> io::Result<()> {
        todo!()
    }
}

impl TreeNode {
    pub fn read_value(&mut self) -> io::Result<Option<Value>> {
        todo!()
    }
}
