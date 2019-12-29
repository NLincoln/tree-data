use super::{InternalPage, Key, LeafPage, Page, PageOffset};
use crate::{Database, Disk};

use std::io;

pub struct BTree {
    root: PageOffset,
}

impl BTree {
    pub fn init_btree_structure<D: Disk>(disk: &mut Database<D>) -> io::Result<BTree> {
        let root = LeafPage::init(disk)?;
        Ok(BTree {
            root: root.offset(),
        })
    }

    pub fn btree_insert<D: Disk>(
        &mut self,
        key: Key,
        data: &[u8],
        db: &mut Database<D>,
    ) -> io::Result<()> {
        let root = Page::load(self.root, db)?;
        if root.is_full(db.block_size()) {
            //            eprintln!("ROOT_FULL [{}]", self.root);
            let mut page = InternalPage::init(db, self.root)?;
            self.root = page.offset();
            //            eprintln!("NEW_ROOT_OFFSET [{}]", page.offset());
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
                let mut i = page.keys().len() - 1;
                while key < page.key(i) {
                    i -= 1;
                }
                i += 1;
                let child = Page::load(page.pointer(i), db)?;
                //                eprintln!(
                //                    "INSERT_NONFULL_INTERNAL [offset={}][i={}][child.offset={}]",
                //                    page.offset(),
                //                    i,
                //                    page.pointer(i)
                //                );
                let child = if child.is_full(db.block_size()) {
                    //                    eprintln!("SPLIT_NONROOT [i={}][page.offset={}]", i, page.offset());
                    self.btree_split_child(&mut page, i, db)?;
                    if key > page.key(i) {
                        i += 1;
                        Page::load(page.pointer(i), db)?
                    } else {
                        child
                    }
                } else {
                    child
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
    ) -> io::Result<()> {
        let left_sibling = Page::load(node.pointer(insert_idx), db)?;
        match left_sibling {
            Page::Leaf(mut left_sibling) => {
                //                eprintln!(
                //                    "SPLIT_LEAF [offset={}][keys_len={}]",
                //                    left_sibling.offset(),
                //                    left_sibling.keys().len()
                //                );
                let new_right_sibling = left_sibling.split_in_half(db)?;
                node.safe_insert(
                    insert_idx,
                    left_sibling.keys().last().unwrap().key,
                    new_right_sibling.offset(),
                    db,
                )?;
                //                eprintln!(
                //                    "SPLIT_LEAF_END [new_sibling={}]",
                //                    new_right_sibling.offset()
                //                );
            }
            Page::Internal(mut left_sibling) => {
                let (new_right_sibling, key) = left_sibling.split_in_half(db)?;
                node.safe_insert(insert_idx, key, new_right_sibling.offset(), db)?;
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
                while i < page.keys().len() && key > page.keys()[i] {
                    i += 1;
                }
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
}

#[cfg(test)]
mod btree_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn btrees_can_have_a_little_test() -> io::Result<()> {
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
