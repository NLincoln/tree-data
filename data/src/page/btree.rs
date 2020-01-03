use super::{InternalPage, Key, LeafPage, Page, PageOffset};
use crate::{Database, Disk};

use std::io;

pub struct BTree {
    root: PageOffset,
}

impl BTree {
    pub fn init<D: Disk>(disk: &mut Database<D>) -> io::Result<BTree> {
        let root = LeafPage::init(disk)?;
        Ok(BTree {
            root: root.offset(),
        })
    }

    pub fn insert<D: Disk>(
        &mut self,
        key: Key,
        data: &[u8],
        db: &mut Database<D>,
    ) -> io::Result<()> {
        let root = Page::load(self.root, db)?;
        if root.can_accommodate(data.len() as u64, db.block_size()) {
            self.btree_insert_nonfull(root, key, data, db)?;
        } else {
            log::debug!("ROOT_FULL [root={}]", self.root);
            let mut page = InternalPage::init(db, self.root)?;
            self.root = page.offset();
            log::debug!("NEW_ROOT_OFFSET [offset={}]", page.offset());
            self.btree_split_child(&mut page, 0, db)?;
            self.btree_insert_nonfull(page.into(), key, data, db)?;
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
                let i = match page.keys().binary_search(&key) {
                    Ok(val) => val,
                    Err(val) => val,
                };
                let child = Page::load(page.pointer(i), db)?;
                log::debug!(
                    "INSERT_NONFULL_INTERNAL [offset={}][i={}][child.offset={}]",
                    page.offset(),
                    i,
                    page.pointer(i)
                );
                let child = if child.can_accommodate(data.len() as u64, db.block_size()) {
                    child
                } else {
                    log::debug!("SPLIT_NONROOT [i={}][page.offset={}]", i, page.offset());
                    let (left_child, right_child) = self.btree_split_child(&mut page, i, db)?;
                    if key > page.key(i) {
                        right_child
                    } else {
                        left_child
                    }
                };
                self.btree_insert_nonfull(child, key, data, db)?;
            }
        };
        Ok(())
    }

    fn btree_split_child<D: Disk>(
        &self,
        node: &mut InternalPage,
        insert_idx: usize,
        db: &mut Database<D>,
    ) -> io::Result<(Page, Page)> {
        let left_sibling = Page::load(node.pointer(insert_idx), db)?;
        match left_sibling {
            Page::Leaf(mut left_sibling) => {
                log::debug!(
                    "SPLIT_LEAF [offset={}][keys_len={}]",
                    left_sibling.offset(),
                    left_sibling.keys().len()
                );
                let new_right_sibling = left_sibling.split_in_half(db)?;
                node.safe_insert(
                    insert_idx,
                    left_sibling.keys().last().unwrap().key,
                    new_right_sibling.offset(),
                    db,
                )?;
                log::debug!(
                    "SPLIT_LEAF_END [new_sibling={}]",
                    new_right_sibling.offset()
                );
                Ok((left_sibling.into(), new_right_sibling.into()))
            }
            Page::Internal(mut left_sibling) => {
                let (new_right_sibling, key) = left_sibling.split_in_half(db)?;
                node.safe_insert(insert_idx, key, new_right_sibling.offset(), db)?;
                Ok((left_sibling.into(), new_right_sibling.into()))
            }
        }
    }

    fn btree_search<D: Disk>(
        &mut self,
        page: Page,
        key: Key,
        db: &mut Database<D>,
    ) -> io::Result<Option<Vec<u8>>> {
        match page {
            Page::Internal(page) => {
                let i = match page.keys().binary_search(&key) {
                    Ok(num) => num,
                    Err(num) => num,
                };
                //                eprintln!(
                //                    "LOOKUP_RECUR [offset={}][i={}][page.pointers[i]={}]",
                //                    page.offset(),
                //                    i,
                //                    page.pointers()[i]
                //                );
                let child = Page::load(page.pointers()[i], db)?;
                self.btree_search(child, key, db)
            }
            Page::Leaf(page) => {
                //                eprintln!("LOOKUP_RECUR_LEAF [offset={}]", page.offset());
                page.lookup_value_alloc(key, &mut db.disk)
            }
        }
    }
    pub fn lookup<D: Disk>(
        &mut self,
        key: Key,
        db: &mut Database<D>,
    ) -> io::Result<Option<Vec<u8>>> {
        let page = Page::load(self.root, db)?;
        return self.btree_search(page, key, db);
    }
    pub fn delete<D: Disk>(&mut self, key: Key, db: &mut Database<D>) -> io::Result<()> {
        let root = Page::load(self.root, db)?;
        match root {
            Page::Leaf(mut leaf) => {
                leaf.delete_value(key, &mut db.disk)?;
            }
            Page::Internal(mut internal) => {
                internal.delete_value(key, db)?;
                if internal.keys().is_empty() {
                    self.root = internal.pointer(0);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod btree_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn btrees_can_have_a_little_test() -> io::Result<()> {
        let mut db = Database::initialize(Cursor::new(vec![]))?;
        let mut tree = BTree::init(&mut db)?;
        let key = 1;
        let data = &[1, 2, 3, 4];
        tree.insert(key, data, &mut db)?;
        assert_eq!(&tree.lookup(key, &mut db)?.unwrap(), data);
        let mut data = vec![0];
        for i in 1..128 {
            data.push(i);
        }
        for key in 1..8_000 {
            data[0] = (key % 40) as u8;
            eprintln!("INSERT [{}]", key);
            tree.insert(key, &data, &mut db)?;
            eprintln!("LOOKUP [{}]", key);

            match tree.lookup(key, &mut db)? {
                Some(found) => assert_eq!(found, data),
                None => panic!("Failed to lookup key {}", key),
            };
        }
        for key in 10..8_000 {
            eprintln!("DELETE [{}]", key);
            tree.delete(key, &mut db)?;
            match tree.lookup(key, &mut db)? {
                Some(_) => panic!("Key was not actually deleted {}", key),
                None => {}
            }
        }
        Ok(())
    }
}
