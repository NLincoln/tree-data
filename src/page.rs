use crate::{BlockAllocator, Database, Disk};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fmt::DebugSet;
use std::io::SeekFrom;
use std::mem::size_of;
use std::{io, num::NonZeroU64};

type PageNumber = NonZeroU64;
type PageOffset = u64;
type Key = u128;

pub struct BTree {
    root: PageOffset,
}

fn safe_insert<T>(vec: &mut Vec<T>, idx: usize, val: T) {
    if idx == vec.len() {
        vec.push(val);
    } else {
        vec.insert(idx, val);
    }
}

impl BTree {
    fn init_btree_structure<D: Disk>(disk: &mut Database<D>) -> io::Result<BTree> {
        let root = LeafPage::init(disk)?;
        Ok(BTree { root: root.offset })
    }

    fn btree_insert<D: Disk>(
        &mut self,
        key: Key,
        data: &[u8],
        db: &mut Database<D>,
    ) -> io::Result<()> {
        let root = Page::load(self.root, db)?;
        if root.is_full(db.block_size()) {
            eprintln!("ROOT_FULL [{}]", self.root);
            let mut page = InternalPage::init_completely_empty(db)?;
            eprintln!("NEW_ROOT_OFFSET [{}]", page.offset);
            page.pointers.push(self.root);
            self.root = page.offset;
            page.persist(db)?;
            self.btree_split_child(&mut page, 0, db)?;
            self.btree_insert_nonfull(page.into(), key, data, db)?;
        } else {
            self.btree_insert_nonfull(root, key, data, db)?;
        }
        Ok(())
    }

    fn btree_insert_nonfull<D: Disk>(
        &mut self,
        page: Page,
        key: Key,
        data: &[u8],
        db: &mut Database<D>,
    ) -> io::Result<()> {
        match page {
            Page::Leaf(mut page) => {
                page.upsert_value(key, data, db)?;
            }
            Page::Internal(mut page) => {
                let mut i = page.keys.len() - 1;
                while key < page.keys[i] {
                    i -= 1;
                }
                i += 1;
                let child = Page::load(page.pointers[i], db)?;
                eprintln!(
                    "INSERT_NONFULL_INTERNAL [offset={}][i={}][child.offset={}]",
                    page.offset, i, page.pointers[i]
                );
                let child = if child.is_full(db.block_size()) {
                    eprintln!("SPLIT_NONROOT [i={}][page.offset={}]", i, page.offset);
                    self.btree_split_child(&mut page, i, db)?;
                    if key > page.keys[i] {
                        i += 1;
                        Page::load(page.pointers[i], db)?
                    } else {
                        child
                    }
                } else {
                    child
                };
                self.btree_insert_nonfull(child, key, data, db);
            }
        };
        Ok(())
    }

    fn btree_split_child<D: Disk>(
        &self,
        node: &mut InternalPage,
        insert_idx: usize,
        db: &mut Database<D>,
    ) -> io::Result<()> {
        let left_sibling = Page::load(node.pointers[insert_idx], db)?;
        match left_sibling {
            Page::Leaf(mut left_sibling) => {
                eprintln!(
                    "SPLIT_LEAF [offset={}][keys_len={}]",
                    left_sibling.offset,
                    left_sibling.keys.len()
                );
                let split_idx = left_sibling.keys.len() / 2;
                let mut new_right_sibling = LeafPage::init(db)?;
                let mut buf = vec![];
                for entry in &left_sibling.keys[split_idx..] {
                    let value = left_sibling.lookup_value(entry.key, &mut buf, &mut db.disk)?;
                    let value = value.expect("could not lookup value");
                    new_right_sibling.upsert_value(entry.key, &buf, db);
                }
                left_sibling.keys.truncate(split_idx);

                safe_insert(&mut node.pointers, insert_idx + 1, new_right_sibling.offset);
                safe_insert(
                    &mut node.keys,
                    insert_idx,
                    left_sibling.keys.last().unwrap().key,
                );
                node.persist(db)?;
                left_sibling.persist_header(&mut db.disk)?;
                new_right_sibling.persist_header(&mut db.disk)?;
                eprintln!("SPLIT_LEAF_END");
            }
            Page::Internal(mut left_sibling) => {
                let split_idx = left_sibling.keys.len() / 2;
                let mut new_right_sibling = InternalPage::init_completely_empty(db)?;
                new_right_sibling.keys = left_sibling.keys.split_off(split_idx);
                new_right_sibling.pointers = left_sibling.pointers.split_off(split_idx);
                node.pointers
                    .insert(insert_idx + 1, new_right_sibling.offset);
                node.keys.insert(
                    insert_idx,
                    left_sibling
                        .keys
                        .pop()
                        .expect("Tried to pop, couldn't get one"),
                );
                node.persist(db)?;
                left_sibling.persist(db)?;
                new_right_sibling.persist(db)?;
            }
        };

        Ok(())
    }

    fn btree_search<D: Disk>(
        &mut self,
        page: Page,
        key: Key,
        db: &mut Database<D>,
    ) -> io::Result<Option<Vec<u8>>> {
        match page {
            Page::Internal(page) => {
                let mut i = 0;
                while i < page.keys.len() && key > page.keys[i] {
                    i += 1;
                }
                eprintln!(
                    "LOOKUP_RECUR [offset={}][i={}][page.pointers[i]={}]",
                    page.offset, i, page.pointers[i]
                );
                let child = Page::load(page.pointers[i], db)?;
                self.btree_search(child, key, db)
            }
            Page::Leaf(page) => {
                eprintln!("LOOKUP_RECUR_LEAF [offset={}]", page.offset);
                page.lookup_value_alloc(key, &mut db.disk)
            }
        }
    }
    fn lookup<D: Disk>(&mut self, key: Key, db: &mut Database<D>) -> io::Result<Option<Vec<u8>>> {
        let page = Page::load(self.root, db)?;
        return self.btree_search(page, key, db);
    }
}

#[cfg(test)]
mod btree_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn btrees_can_have_a_little_test() -> io::Result<()> {
        let page_size = 2u64.pow(14);
        let mut db = Database::initialize(Cursor::new(vec![]))?;
        let mut tree = BTree::init_btree_structure(&mut db)?;
        let key = 1;
        let data = &[1, 2, 3, 4];
        tree.btree_insert(key, data, &mut db)?;
        assert_eq!(&tree.lookup(key, &mut db)?.unwrap(), data);
        let mut data = vec![0];
        for i in 1..128 {
            data.push(i);
        }
        for key in 1..8_000 {
            data[0] = (key % 40) as u8;
            eprintln!("INSERT [{}]", key);
            tree.btree_insert(key, &data, &mut db)?;
            eprintln!("LOOKUP [{}]", key);

            match tree.lookup(key, &mut db)? {
                Some(found) => assert_eq!(found, data),
                None => panic!("Failed to lookup key {}", key),
            };
        }
        Ok(())
    }
}

enum Page {
    Internal(InternalPage),
    Leaf(LeafPage),
}

impl Into<Page> for LeafPage {
    fn into(self) -> Page {
        Page::Leaf(self)
    }
}

impl Into<Page> for InternalPage {
    fn into(self) -> Page {
        Page::Internal(self)
    }
}

impl Page {
    const LEAF_TAG: u8 = 0x01;
    const INTERNAL_TAG: u8 = 0x02;
    fn load<D: Disk>(offset: u64, db: &mut Database<D>) -> io::Result<Page> {
        let disk = &mut db.disk;
        disk.seek(SeekFrom::Start(offset))?;
        let tag = disk.read_u8()?;
        disk.seek(SeekFrom::Start(offset))?;
        let page: Page = match tag {
            Page::LEAF_TAG => LeafPage::read_header(disk)?.into(),
            Page::INTERNAL_TAG => InternalPage::load(db)?.into(),
            n => {
                panic!("Unknown page tag {}", n);
            }
        };
        Ok(page)
    }
    fn is_full(&self, page_size: u64) -> bool {
        match self {
            Page::Internal(internal) => internal.is_full(page_size),
            Page::Leaf(leaf) => leaf.is_full(page_size),
        }
    }
}

#[derive(Clone, Debug)]
struct LeafPageEntry {
    key: Key,
    offset: PageOffset,
    value_len: u64,
}

impl LeafPageEntry {
    fn end_offset(&self) -> PageOffset {
        self.offset + self.value_len
    }
    const fn size_of_entry() -> u64 {
        // this function is const, so it doesn't
        // really matter what work we do here
        let key_size = std::mem::size_of::<Key>() as u64;
        let other_size = std::mem::size_of::<u64>() as u64;
        key_size + other_size * 2
    }
}

pub(crate) struct LeafPage {
    offset: u64,
    keys: Vec<LeafPageEntry>,
}

impl LeafPage {
    const DESIRED_FANOUT: u64 = 8;
    fn max_size_per_element(page_size: u64) -> u64 {
        page_size / Self::DESIRED_FANOUT
    }
    fn seek_to_offset(&self, disk: &mut impl Disk) -> io::Result<()> {
        disk.seek(SeekFrom::Start(self.offset))?;
        Ok(())
    }
    pub(crate) fn persist_header(&self, disk: &mut impl Disk) -> io::Result<()> {
        self.seek_to_offset(disk)?;
        disk.write_u8(Page::LEAF_TAG)?;
        disk.write_u64::<BigEndian>(self.keys.len() as u64)?;
        for entry in self.keys.iter() {
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

    pub(crate) fn has_key(&self, key: Key) -> bool {
        self.keys.iter().any(|entry| entry.key == key)
    }

    fn is_full(&self, page_size: u64) -> bool {
        if self.keys.is_empty() {
            return false;
        }
        let mut last_entry_offset = self.header_len();

        let entries_sorted_by_offset = {
            let mut keys = self.keys.clone();
            keys.sort_by_key(|entry| entry.offset);
            keys
        };

        // Header is the size of the existing entries, plus
        // the size with the new entry added.
        let mut last_entry_end_point = self.header_len() + LeafPageEntry::size_of_entry();
        let mut total_space_found = 0;
        for entry in entries_sorted_by_offset {
            let len_of_this_slice = entry.offset - last_entry_end_point;
            total_space_found += len_of_this_slice;
            last_entry_end_point = entry.end_offset();
        }
        return total_space_found < LeafPage::max_size_per_element(page_size);
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

    pub(crate) fn upsert_value<D: Disk>(
        &mut self,
        key: Key,
        data: &[u8],
        db: &mut Database<D>,
    ) -> io::Result<()> {
        let page_size = db.block_size();
        let disk = &mut db.disk;
        self.seek_to_offset(disk)?;
        // I'm going to use the first-fit algorithm for this, since it's the easiest to
        // implement.
        fn push_entry(
            page: &mut LeafPage,
            disk: &mut impl Disk,
            key: Key,
            data: &[u8],
            entry: LeafPageEntry,
        ) -> io::Result<()> {
            disk.seek(SeekFrom::Start(page.offset + entry.offset))?;
            disk.write_all(data)?;
            match page.keys.binary_search_by_key(&key, |entry| entry.key) {
                Ok(_) => unreachable!(),
                Err(idx) => page.keys.insert(idx, entry),
            }
            page.persist_header(disk)?;
            eprintln!("INSERT_COMMIT [offset={}][key={}]", page.offset, key);
            return Ok(());
        }

        // First of all, if there are no entries yet, then the entire region is open.
        // this is... a bit rare? Should only happen on the very first insert
        // into a given table, but I suppose it's possible.
        if self.keys.is_empty() {
            return push_entry(
                self,
                disk,
                key,
                data,
                LeafPageEntry {
                    offset: page_size - data.len() as u64,
                    key,
                    value_len: data.len() as u64,
                },
            );
        }

        let entries_sorted_by_offset = {
            let mut keys = self.keys.clone();
            keys.sort_by_key(|entry| entry.offset);
            keys
        };

        // Header is the size of the existing entries, plus
        // the size with the new entry added.
        let mut last_entry_end_point = self.header_len() + LeafPageEntry::size_of_entry();

        for entry in entries_sorted_by_offset {
            if key == entry.key {
                self.delete_value(key, disk)?;
                return self.upsert_value(key, data, db);
            }
            let len_of_this_slice = entry.offset - last_entry_end_point;
            if len_of_this_slice >= data.len() as u64 {
                // we have found a viable entry point!
                return push_entry(
                    self,
                    disk,
                    key,
                    data,
                    LeafPageEntry {
                        offset: entry.offset - data.len() as u64,
                        key,
                        value_len: data.len() as u64,
                    },
                );
            }
            last_entry_end_point = entry.end_offset();
        }
        panic!("No entries found in page. Page may be fragmented or too full");
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
}

#[cfg(test)]
mod tests_leafpage {
    use super::*;
    use std::io::{Cursor, Seek};

    #[test]
    fn test_leaf_page_a_bit() -> io::Result<()> {
        let mut db = Database::initialize(Cursor::new(vec![]))?;
        let page_size = db.block_size();

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
        let page_size = db.block_size();
        let mut page = LeafPage::init(&mut db)?;
        db.disk.seek(SeekFrom::Start(0))?;
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
}

struct InternalPage {
    offset: u64,
    keys: Vec<Key>,
    pointers: Vec<PageOffset>,
}

impl InternalPage {
    fn max_children_capacity(page_size: u64) -> u64 {
        // Solve[pageSize==head+n*childSize+(n-1)*keySize,n]
        let head_size = Self::header_size();
        let child_ptr_size = size_of::<PageOffset>() as u64;
        let key_size = size_of::<Key>() as u64;
        (page_size + key_size - head_size) / (child_ptr_size + key_size)
    }
    fn is_full(&self, page_size: u64) -> bool {
        self.pointers.len() as u64 >= InternalPage::max_children_capacity(page_size)
    }
    fn header_size() -> u64 {
        size_of::<u8>() as u64 + size_of::<u64>() as u64
    }
    fn init_completely_empty<D: Disk>(disk: &mut Database<D>) -> io::Result<InternalPage> {
        let offset = disk.allocate_block()?;
        let page_size = disk.block_size();
        let mut buf = vec![0u8; page_size as usize];
        buf[0] = Page::INTERNAL_TAG;
        disk.write(offset, &buf)?;
        Ok(InternalPage {
            offset,
            keys: vec![],
            pointers: vec![],
        })
    }
    fn load<D: Disk>(db: &mut Database<D>) -> io::Result<InternalPage> {
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
    fn persist<D: Disk>(&self, db: &mut Database<D>) -> io::Result<()> {
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
