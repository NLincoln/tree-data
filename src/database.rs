use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Seek, SeekFrom, Write};

pub trait Disk: Read + Write + Seek {}
impl<T: Read + Write + Seek> Disk for T {}

pub struct Database<D: Disk> {
    disk: D,
    meta: DatabaseMeta,
}

struct DatabaseMeta {
    block_size_exp: u64,
    num_blocks_allocated: u64,
}
impl<D: Disk> Database<D> {
    pub fn from_existing(mut disk: D) -> io::Result<Self> {
        let meta = Database::read_header(&mut disk)?;
        Ok(Database { disk, meta })
    }

    pub fn initialize(mut disk: D) -> io::Result<Self> {
        let meta = Self::init_header(&mut disk)?;
        Ok(Database { disk, meta })
    }

    pub fn read_record(&mut self, table_id: u128) -> io::Result<()> {
        unimplemented!();
    }

    fn read_header(disk: &mut D) -> io::Result<DatabaseMeta> {
        disk.seek(SeekFrom::Start(0))?;
        let block_size_exp = disk.read_u64::<BigEndian>()?;
        let num_blocks_allocated = disk.read_u64::<BigEndian>()?;
        Ok(DatabaseMeta {
            block_size_exp,
            num_blocks_allocated,
        })
    }

    fn init_header(disk: &mut D) -> io::Result<DatabaseMeta> {
        disk.seek(SeekFrom::Start(0))?;
        let block_size_exp = 16u64;
        disk.write_u64::<BigEndian>(block_size_exp)?;
        let num_blocks_allocated = 0u64;
        disk.write_u64::<BigEndian>(num_blocks_allocated)?;
        let meta = DatabaseMeta {
            block_size_exp,
            num_blocks_allocated,
        };
        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    fn cursor() -> impl Disk {
        Cursor::new(vec![])
    }
    fn database() -> Database<impl Disk> {
        Database::initialize(cursor()).unwrap()
    }

    #[test]
    fn create_new_database() {
        database();
    }

    #[test]
    fn insert_and_retrieve() -> io::Result<()> {
        let table_id = 0u128;
        let id = 1;
        let data = &[0, 1, 2, 3];
        let mut db = database();
        Ok(())
    }
}
