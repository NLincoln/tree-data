use crate::{Database, Disk};

use byteorder::ReadBytesExt;
use std::io;
use std::io::SeekFrom;
mod btree;
mod internal_page;
mod leaf_page;

pub use btree::BTree;
use internal_page::InternalPage;
use leaf_page::LeafPage;

type PageOffset = u64;
use crate::Key;

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
    fn can_accommodate(&self, data_len: u64, page_size: u64) -> bool {
        match self {
            Page::Internal(internal) => internal.can_accommodate(page_size),
            Page::Leaf(leaf) => leaf.can_accommodate(data_len, page_size),
        }
    }
}
