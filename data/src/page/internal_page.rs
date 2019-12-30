use super::{Key, Page, PageOffset};
use crate::{BlockAllocator, Database, Disk};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use std::{
    io::{self, SeekFrom},
    mem::size_of,
};

pub struct InternalPage {
    offset: u64,
    keys: Vec<Key>,
    pointers: Vec<PageOffset>,
}
fn safe_insert<T>(vec: &mut Vec<T>, idx: usize, val: T) {
    if idx == vec.len() {
        vec.push(val);
    } else {
        vec.insert(idx, val);
    }
}

impl InternalPage {
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn keys(&self) -> &[Key] {
        &self.keys
    }
    pub fn pointers(&self) -> &[PageOffset] {
        &self.pointers
    }
    pub fn key(&self, i: usize) -> Key {
        self.keys[i]
    }
    pub fn pointer(&self, i: usize) -> PageOffset {
        self.pointers[i]
    }
    pub fn safe_insert<D: Disk>(
        &mut self,
        i: usize,
        key: Key,
        pointer: PageOffset,
        db: &mut Database<D>,
    ) -> io::Result<()> {
        safe_insert(&mut self.keys, i, key);
        safe_insert(&mut self.pointers, i + 1, pointer);
        self.persist(db)
    }
    pub fn safe_remove<D: Disk>(&mut self, i: usize, db: &mut Database<D>) -> io::Result<()> {
        self.keys.remove(i);
        self.pointers.remove(i + 1);
        self.persist(db)
    }
    fn max_children_capacity(page_size: u64) -> u64 {
        // Solve[pageSize==head+n*childSize+(n-1)*keySize,n]
        let head_size = Self::header_size();
        let child_ptr_size = size_of::<PageOffset>() as u64;
        let key_size = size_of::<Key>() as u64;
        (page_size + key_size - head_size) / (child_ptr_size + key_size)
    }
    pub fn is_full(&self, page_size: u64) -> bool {
        self.pointers.len() as u64 >= InternalPage::max_children_capacity(page_size)
    }
    fn header_size() -> u64 {
        size_of::<u8>() as u64 + size_of::<u64>() as u64
    }
    pub fn init<D: Disk>(db: &mut Database<D>, pointer: PageOffset) -> io::Result<InternalPage> {
        let offset = db.allocate_block()?;
        let page = InternalPage {
            offset,
            keys: vec![],
            pointers: vec![pointer],
        };
        page.persist(db)?;
        Ok(page)
    }
    pub fn split_in_half<D: Disk>(
        &mut self,
        db: &mut Database<D>,
    ) -> io::Result<(InternalPage, Key)> {
        let split_idx = self.keys.len() / 2;
        let offset = db.allocate_block()?;

        let new_right_sibling = InternalPage {
            offset,
            keys: self.keys.split_off(split_idx),
            pointers: self.pointers.split_off(split_idx),
        };

        let key = self.keys.pop().unwrap();

        new_right_sibling.persist(db)?;
        self.persist(db)?;
        Ok((new_right_sibling, key))
    }
    pub fn delete_value<D: Disk>(&mut self, key: Key, db: &mut Database<D>) -> io::Result<()> {
        let i = match self.keys.binary_search(&key) {
            Ok(val) => val,
            Err(val) => val,
        };
        eprintln!("INTERNAL_DELETE_VALUE [i={}][ptr={}]", i, self.pointer(i));
        let child = Page::load(self.pointer(i), db)?;
        match child {
            Page::Leaf(mut leaf) => {
                eprintln!("DELETE_LEAF_VALUE");
                leaf.delete_value(key, &mut db.disk)?;
                if leaf.keys().is_empty() {
                    let idx_to_remove = if i == 0 { 0 } else { i - 1 };
                    self.safe_remove(idx_to_remove, db)?;
                }
            }
            Page::Internal(mut internal) => {
                internal.delete_value(key, db)?;
                if internal.keys.is_empty() {
                    self.pointers[i] = internal.pointer(0);
                    self.persist(db)?;
                }
            }
        }

        Ok(())
    }
    pub fn load<D: Disk>(db: &mut Database<D>) -> io::Result<InternalPage> {
        let disk = &mut db.disk;
        let offset = disk.seek(SeekFrom::Current(0))?;
        let tag = disk.read_u8()?;
        assert_eq!(tag, Page::INTERNAL_TAG);
        let keys_len = disk.read_u64::<BigEndian>()? as usize;
        let mut keys = Vec::with_capacity(keys_len);
        for _ in 0..keys_len {
            keys.push(disk.read_u128::<BigEndian>()?);
        }
        let mut pointers = Vec::with_capacity(keys_len + 1);
        for _ in 0..(keys_len + 1) {
            pointers.push(disk.read_u64::<BigEndian>()?)
        }
        Ok(InternalPage {
            offset,
            keys,
            pointers,
        })
    }
    pub fn persist<D: Disk>(&self, db: &mut Database<D>) -> io::Result<()> {
        assert!(InternalPage::max_children_capacity(db.block_size()) >= self.pointers.len() as u64);
        let disk = &mut db.disk;
        disk.seek(SeekFrom::Start(self.offset))?;
        let keys_len = self.keys.len();
        disk.write_u8(Page::INTERNAL_TAG)?;
        disk.write_u64::<BigEndian>(keys_len as u64)?;
        assert_eq!(self.pointers.len(), keys_len + 1);
        for &key in self.keys.iter() {
            disk.write_u128::<BigEndian>(key)?;
        }
        for &ptr in self.pointers.iter() {
            disk.write_u64::<BigEndian>(ptr)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod internal_page_tests {
    use super::*;
    #[test]
    fn test_max_child_capacity() {
        assert_eq!(InternalPage::max_children_capacity(2048), 85);
        assert_eq!(InternalPage::max_children_capacity(4096), 170);
    }
}
