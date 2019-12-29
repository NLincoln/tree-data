use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::convert::TryInto;
use std::io::{self, Read, Seek, SeekFrom, Write};

pub trait Disk: Read + Write + Seek {}
impl<T: Read + Write + Seek> Disk for T {}

pub trait BlockAllocator {
    fn allocate_block(&mut self) -> io::Result<u64>;
    fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()>;
}

pub struct Database<D: Disk> {
    pub(crate) disk: D,
    meta: DatabaseMeta,
}

struct DatabaseMeta {
    block_size_exp: u64,
    num_blocks_allocated: u64,
}

impl DatabaseMeta {
    fn block_size(&self) -> u64 {
        2u64.pow(self.block_size_exp.try_into().unwrap())
    }
    fn persist(&self, disk: &mut impl Disk) -> io::Result<()> {
        disk.seek(SeekFrom::Start(0))?;
        disk.write_u64::<BigEndian>(self.block_size_exp)?;
        disk.write_u64::<BigEndian>(self.num_blocks_allocated)?;
        Ok(())
    }
}

impl<D: Disk> Database<D> {
    pub fn block_size(&self) -> u64 {
        self.meta.block_size()
    }
    pub fn from_existing(mut disk: D) -> io::Result<Self> {
        let meta = Database::read_header(&mut disk)?;
        Ok(Database { disk, meta })
    }

    pub fn initialize(mut disk: D) -> io::Result<Self> {
        let meta = Self::init_header(&mut disk)?;
        Ok(Database { disk, meta })
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
        let block_size_exp = 13u64;
        // 1 for the meta block
        let num_blocks_allocated = 1u64;
        let meta = DatabaseMeta {
            block_size_exp,
            num_blocks_allocated,
        };
        meta.persist(disk)?;
        Ok(meta)
    }
}

impl<D: Disk> BlockAllocator for Database<D> {
    fn allocate_block(&mut self) -> io::Result<u64> {
        let block_size = self.meta.block_size();
        let new_offset = block_size * self.meta.num_blocks_allocated;
        self.meta.num_blocks_allocated += 1;
        self.meta.persist(&mut self.disk)?;
        Ok(new_offset)
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.disk.seek(SeekFrom::Start(offset))?;
        self.disk.write_all(data)?;
        Ok(())
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
