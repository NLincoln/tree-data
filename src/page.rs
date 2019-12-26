use crate::{Database, Disk};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::convert::TryInto;
use std::io::SeekFrom;
use std::ops::Add;
use std::{io, num::NonZeroU64};

type PageNumber = NonZeroU64;
type Key = u128;

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

#[derive(Clone)]
struct LeafPageEntry {
    key: Key,
    offset: u64,
    value_len: u64,
}

impl LeafPageEntry {
    fn end_offset(&self) -> u64 {
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
    keys: Vec<LeafPageEntry>,
}

impl LeafPage {
    pub(crate) fn persist_header(&self, disk: &mut impl Disk) -> io::Result<()> {
        disk.write_u64::<BigEndian>(self.keys.len() as u64)?;
        for entry in self.keys.iter() {
            disk.write_u128::<BigEndian>(entry.key)?;
            disk.write_u64::<BigEndian>(entry.offset)?;
            disk.write_u64::<BigEndian>(entry.value_len)?;
        }
        Ok(())
    }
    pub(crate) fn read_header(disk: &mut impl Disk) -> io::Result<LeafPage> {
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
        Ok(LeafPage { keys: buf })
    }

    fn header_len(&self) -> u64 {
        LeafPageEntry::size_of_entry() * self.keys.len() as u64 + std::mem::size_of::<u64>() as u64
    }

    pub(crate) fn has_key(&self, key: Key) -> bool {
        self.keys.iter().any(|entry| entry.key == key)
    }

    pub(crate) fn lookup_value(
        &self,
        key: Key,
        data: &mut [u8],
        disk: &mut impl Disk,
    ) -> io::Result<Option<u64>> {
        let entry = self.keys.iter().find(|entry| entry.key == key);
        let entry = match entry {
            Some(entry) => entry,
            None => return Ok(None),
        };
        assert!(data.len() >= entry.value_len as usize);
        disk.seek(SeekFrom::Current(entry.offset as i64))?;
        disk.read_exact(&mut data[0..entry.value_len as usize])?;
        return Ok(Some(entry.value_len));
    }

    pub(crate) fn delete_value(&mut self, key: Key, disk: &mut impl Disk) -> io::Result<bool> {
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

    pub(crate) fn insert_value(
        &mut self,
        key: Key,
        data: &[u8],
        page_size: u64,
        disk: &mut impl Disk,
    ) -> io::Result<()> {
        // So we assume that our current offset is at the start of the page. This might not
        // be entirely true, but uhhh let's go with it. Offsets are going to be hard anyways
        // with this design, why not just not worry about them :)
        let page_offset = disk.seek(SeekFrom::Current(0))?;
        // I'm going to use the first-fit algorithm for this, since it's the easiest to
        // implement.

        // First of all, if there are no entries yet, then the entire region is open.
        // this is... a bit rare? Should only happen on the very first insert
        // into a given table, but I suppose it's possible.
        if self.keys.is_empty() {
            let new_entry = LeafPageEntry {
                offset: page_size - data.len() as u64,
                key,
                value_len: data.len() as u64,
            };
            disk.seek(SeekFrom::Start(page_offset + new_entry.offset))?;
            disk.write_all(data)?;
            disk.seek(SeekFrom::Start(page_offset))?;
            self.keys.push(new_entry);
            self.persist_header(disk)?;
            return Ok(());
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
                panic!("Key {} already present in the database, cannot insert", key);
            }
            let len_of_this_slice = entry.offset - last_entry_end_point;
            if len_of_this_slice >= data.len() as u64 {
                // we have found a viable entry point!
                let new_entry = LeafPageEntry {
                    offset: entry.offset - data.len() as u64,
                    key,
                    value_len: data.len() as u64,
                };
                disk.seek(SeekFrom::Start(page_offset + new_entry.offset))?;
                disk.write_all(data)?;
                disk.seek(SeekFrom::Start(page_offset))?;
                match self.keys.binary_search_by_key(&key, |entry| entry.key) {
                    Ok(_) => unreachable!(),
                    Err(idx) => self.keys.insert(idx, new_entry),
                }
                self.persist_header(disk)?;
                return Ok(());
            }
            last_entry_end_point = entry.end_offset();
        }
        panic!("No entries found in page. Page may be fragmented or too full");
    }
    pub(crate) fn init(page_size: u64, disk: &mut impl Disk) -> io::Result<LeafPage> {
        // idk we just need to write a nice page_size buffer to the disk
        let buf = vec![0u8; page_size as usize];
        disk.write_all(&buf)?;
        Ok(LeafPage { keys: vec![] })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Seek};

    #[test]
    fn test_leaf_page_a_bit() -> io::Result<()> {
        let page_size = 2usize.pow(16);
        let mut buf = vec![0u8; page_size];
        let mut disk = Cursor::new(buf.as_mut_slice());
        let mut page = LeafPage::init(page_size as u64, &mut disk)?;
        for i in 0..5 {
            disk.seek(SeekFrom::Start(0))?;
            page.insert_value(i, &[0, 1, 2, 3], page_size as u64, &mut disk)?;
        }
        for i in 2..4 {
            disk.seek(SeekFrom::Start(0))?;
            let mut buf = &mut [0u8; 4];
            page.lookup_value(i, buf, &mut disk)?;
            assert_eq!(buf, &[0, 1, 2, 3]);
        }
        for i in 3..5 {
            disk.seek(SeekFrom::Start(0))?;
            assert!(page.delete_value(i, &mut disk)?);
        }
        Ok(())
    }
}
struct InternalPage {
    keys: Vec<Key>,
    pointers: Vec<PageNumber>,
}
