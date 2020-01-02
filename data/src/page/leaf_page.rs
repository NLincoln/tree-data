use super::{Key, Page, PageOffset};
use crate::{BlockAllocator, Database, Disk};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use std::{
    io::{self, SeekFrom},
    mem::size_of,
};

#[derive(Clone, Debug)]
pub struct LeafPageEntry {
    pub key: Key,
    pub offset: PageOffset,
    pub value_len: u64,
}

impl LeafPageEntry {
    const fn size_of_entry() -> u64 {
        // this function is const, so it doesn't
        // really matter what work we do here
        let key_size = std::mem::size_of::<Key>() as u64;
        let other_size = std::mem::size_of::<u64>() as u64;
        key_size + other_size * 2
    }
}

pub struct LeafPage {
    offset: u64,
    keys: Vec<LeafPageEntry>,
}

impl LeafPage {
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn keys(&self) -> &[LeafPageEntry] {
        &self.keys
    }
    fn seek_to_offset(&self, disk: &mut impl Disk) -> io::Result<()> {
        disk.seek(SeekFrom::Start(self.offset))?;
        Ok(())
    }
    pub(crate) fn persist_header(&self, disk: &mut impl Disk) -> io::Result<()> {
        log::debug!(
            "PERSIST_HEADER [offset={}][keys_len={}]",
            self.offset,
            self.keys.len()
        );
        self.persist_header_offset(disk, 0)
    }
    fn persist_header_offset(&self, disk: &mut impl Disk, offset: usize) -> io::Result<()> {
        self.seek_to_offset(disk)?;
        disk.write_u8(Page::LEAF_TAG)?;
        disk.write_u64::<BigEndian>(self.keys.len() as u64)?;
        disk.seek(SeekFrom::Current(
            (offset as u64 * LeafPageEntry::size_of_entry()) as i64,
        ))?;
        for entry in self.keys.iter().skip(offset) {
            disk.write_u128::<BigEndian>(entry.key)?;
            disk.write_u64::<BigEndian>(entry.offset)?;
            disk.write_u64::<BigEndian>(entry.value_len)?;
        }
        Ok(())
    }
    pub(crate) fn read_header(disk: &mut impl Disk) -> io::Result<LeafPage> {
        let offset = disk.seek(SeekFrom::Current(0))?;
        assert_eq!(disk.read_u8()?, Page::LEAF_TAG);
        let len = disk.read_u64::<BigEndian>()?;
        let mut buf = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let key = disk.read_u128::<BigEndian>()?;
            let offset = disk.read_u64::<BigEndian>()?;
            let value_len = disk.read_u64::<BigEndian>()?;
            buf.push(LeafPageEntry {
                key,
                offset,
                value_len,
            });
        }
        Ok(LeafPage { offset, keys: buf })
    }

    fn header_len(&self) -> u64 {
        LeafPageEntry::size_of_entry() * self.keys.len() as u64
            + std::mem::size_of::<u64>() as u64
            + size_of::<u8>() as u64
    }

    pub fn can_accommodate(&self, data_len: u64, page_size: u64) -> bool {
        if self.keys.is_empty() {
            return true;
        }
        let space_taken_up: u64 = self.keys.iter().map(|entry| entry.value_len).sum();
        let space_in_page_for_data = {
            let header_stop_offset = self.header_len();
            page_size - header_stop_offset
        };
        let space_available = space_in_page_for_data - space_taken_up;
        return space_available >= data_len + LeafPageEntry::size_of_entry();
    }

    pub(crate) fn lookup_value(
        &self,
        key: Key,
        data: &mut Vec<u8>,
        disk: &mut impl Disk,
    ) -> io::Result<Option<u64>> {
        self.seek_to_offset(disk)?;
        let entry = self.keys.iter().find(|entry| entry.key == key);
        let entry = match entry {
            Some(entry) => entry,
            None => return Ok(None),
        };

        disk.seek(SeekFrom::Current(entry.offset as i64))?;
        data.resize(entry.value_len as usize, 0);
        disk.read_exact(&mut data[..])?;
        return Ok(Some(entry.value_len));
    }

    pub(crate) fn lookup_value_alloc(
        &self,
        key: Key,
        disk: &mut impl Disk,
    ) -> io::Result<Option<Vec<u8>>> {
        let entry = self.keys.iter().find(|entry| entry.key == key);
        let entry = match entry {
            Some(entry) => entry,
            None => return Ok(None),
        };
        let mut data = vec![0u8; entry.value_len as usize];
        disk.seek(SeekFrom::Start(self.offset + entry.offset))?;
        disk.read_exact(&mut data)?;
        return Ok(Some(data));
    }

    pub(crate) fn delete_value(&mut self, key: Key, disk: &mut impl Disk) -> io::Result<bool> {
        self.seek_to_offset(disk)?;
        if self.keys.is_empty() {
            return Ok(false);
        }
        let mut key_idx = None;
        for (i, entry) in self.keys.iter().enumerate() {
            if entry.key == key {
                key_idx = Some(i);
                break;
            }
        }
        let key_idx = match key_idx {
            Some(val) => val,
            None => return Ok(false),
        };
        self.keys.remove(key_idx);
        self.persist_header(disk)?;
        Ok(true)
    }

    fn quick_insert<D: Disk>(
        &mut self,
        key: Key,
        data: &[u8],
        db: &mut Database<D>,
        end_offset: Option<u64>,
    ) -> io::Result<()> {
        let page_size = db.block_size();
        let disk = &mut db.disk;
        let end_offset = end_offset.unwrap_or_else(|| {
            self.keys
                .iter()
                .map(|entry| entry.offset)
                .min()
                .unwrap_or(page_size)
        });
        let entry = LeafPageEntry {
            offset: end_offset - data.len() as u64,
            key,
            value_len: data.len() as u64,
        };
        disk.seek(SeekFrom::Start(self.offset + entry.offset))?;
        disk.write_all(data)?;
        match self.keys.binary_search_by_key(&key, |entry| entry.key) {
            Ok(_) => unreachable!(),
            Err(idx) => {
                self.keys.insert(idx, entry);
                self.persist_header(disk)?;
            }
        }
        log::debug!("INSERT_COMMIT [offset={}][key={}]", self.offset, key);
        return Ok(());
    }

    fn defragment<D: Disk>(&mut self, db: &mut Database<D>) -> io::Result<()> {
        log::debug!("DEFRAGMENT");
        let pairs = self
            .keys
            .iter()
            .map(|entry| {
                Ok((
                    entry.key,
                    self.lookup_value_alloc(entry.key, &mut db.disk)?.unwrap(),
                ))
            })
            .collect::<io::Result<Vec<(Key, Vec<u8>)>>>()?;
        self.keys.clear();
        for (key, value) in pairs {
            self.upsert_value(key, &value, db)?;
        }
        Ok(())
    }

    pub(crate) fn upsert_value<D: Disk>(
        &mut self,
        key: Key,
        data: &[u8],
        db: &mut Database<D>,
    ) -> io::Result<()> {
        log::debug!(
            "LEAF_UPSERT_BEGIN [offset={}][key={}][keys_len={}]",
            self.offset,
            key,
            self.keys.len()
        );
        if self.keys.iter().any(|entry| entry.key == key) {
            self.delete_value(key, &mut db.disk)?;
            return self.upsert_value(key, data, db);
        }

        let page_size = db.block_size();
        assert!(self.can_accommodate(data.len() as u64, page_size));
        let end_offset = self
            .keys
            .iter()
            .map(|entry| entry.offset)
            .min()
            .unwrap_or(page_size);
        let start_offset = self.header_len() + LeafPageEntry::size_of_entry();
        if start_offset > end_offset || (end_offset - start_offset < data.len() as u64) {
            self.defragment(db)?;
            return self.upsert_value(key, data, db);
        }
        return self.quick_insert(key, data, db, Some(end_offset));
    }
    pub(crate) fn init<D: Disk>(db: &mut Database<D>) -> io::Result<LeafPage> {
        let page_size = db.block_size();
        let offset = db.allocate_block()?;
        // idk we just need to write a nice page_size buffer to the disk
        let mut buf = vec![0u8; page_size as usize];
        buf[0] = Page::LEAF_TAG;
        db.write(offset, &buf)?;
        Ok(LeafPage {
            offset,
            keys: vec![],
        })
    }
    pub fn split_in_half<D: Disk>(&mut self, db: &mut Database<D>) -> io::Result<LeafPage> {
        let keys_len = self.keys.len();
        let split_idx = keys_len / 2;
        let mut new_right_sibling = LeafPage::init(db)?;
        let mut buf = vec![];
        for entry in &self.keys[split_idx..] {
            let value = self.lookup_value(entry.key, &mut buf, &mut db.disk)?;
            value.expect("could not lookup value");
            new_right_sibling.upsert_value(entry.key, &buf, db)?;
        }
        self.keys.truncate(split_idx);
        self.persist_header(&mut db.disk)?;
        log::debug!(
            "SPLIT_IN_HALF [offset={}][split_idx={}][old_len={}][new_len={}]",
            self.offset,
            split_idx,
            keys_len,
            self.keys.len()
        );
        Ok(new_right_sibling)
    }
}

#[cfg(test)]
mod tests_leafpage {
    use super::*;
    use std::io::{Cursor, Seek};

    #[test]
    fn test_leaf_page_a_bit() -> io::Result<()> {
        let mut db = Database::initialize(Cursor::new(vec![]))?;

        let mut page = LeafPage::init(&mut db)?;
        for i in 0..5 {
            page.upsert_value(i, &[0, 1, 2, 3], &mut db)?;
        }
        for i in 2..4 {
            let mut buf = vec![];
            page.lookup_value(i, &mut buf, &mut db.disk)?;
            assert_eq!(buf, &[0, 1, 2, 3]);
        }
        for i in 3..5 {
            assert!(page.delete_value(i, &mut db.disk)?);
        }
        Ok(())
    }
    #[test]
    fn test_upsert() -> io::Result<()> {
        let mut db = Database::initialize(Cursor::new(vec![]))?;
        let mut page = LeafPage::init(&mut db)?;
        page.upsert_value(0, &[0, 1, 2, 3], &mut db)?;
        page.upsert_value(0, &[1, 2], &mut db)?;

        let mut buf = vec![];
        page.lookup_value(0, &mut buf, &mut db.disk)?;
        assert_eq!(buf, &[1, 2]);

        page.upsert_value(0, &[2, 3, 4, 5], &mut db)?;

        page.lookup_value(0, &mut buf, &mut db.disk)?;
        assert_eq!(buf, &[2, 3, 4, 5]);

        Ok(())
    }
    #[test]
    fn test_split() -> io::Result<()> {
        let mut db = Database::initialize(Cursor::new(vec![]))?;
        let mut page = LeafPage::init(&mut db)?;
        for i in 0..100 {
            page.upsert_value(i, &[0, 1, 2, 3], &mut db)?;
        }
        let new_right_sibling = page.split_in_half(&mut db)?;
        db.disk.seek(SeekFrom::Start(page.offset))?;
        let page = LeafPage::read_header(&mut db.disk)?;
        assert_eq!(page.keys.len(), 50);

        db.disk.seek(SeekFrom::Start(new_right_sibling.offset))?;
        let new_right_sibling = LeafPage::read_header(&mut db.disk)?;
        assert_eq!(new_right_sibling.keys.len(), 50);

        Ok(())
    }
}
