use crate::{BTree, Database, Disk, Key};
use std::convert::TryInto;
use std::io;

pub struct TreeEntry<'d, D: Disk> {
    pub(crate) db: &'d mut Database<D>,
    pub(crate) offset: u64,
}

fn read_be_u64(input: &[u8]) -> u64 {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<u64>());
    u64::from_be_bytes(int_bytes.try_into().unwrap())
}

struct TreeEntryValue {
    child_offset: Option<std::num::NonZeroU64>,
    data: Option<Vec<u8>>,
}

impl TreeEntryValue {
    fn from_data(mut data: Vec<u8>) -> TreeEntryValue {
        if data.len() < 8 {
            data.resize_with(8, Default::default);
        }
        let (data, child_offset) = {
            /*
            First 8 bytes of data is the offset. We know it has 8 bytes
            because of the above condition.
            split_at returns the bytes _after_ the index, which is
            actually the data, so we have to do some awkwardness
            to shuffle everything around
            */
            let mut all_buf = data;
            let data = all_buf.split_off(std::mem::size_of::<u64>());
            (data, all_buf)
        };

        let child_offset = read_be_u64(&child_offset[..]);
        let child_offset = std::num::NonZeroU64::new(child_offset);
        TreeEntryValue {
            child_offset,
            data: if data.len() > 0 { Some(data) } else { None },
        }
    }
    fn new() -> TreeEntryValue {
        TreeEntryValue {
            child_offset: None,
            data: None,
        }
    }
    fn into_buf(self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend_from_slice(
            self.child_offset
                .map(|val| val.get())
                .unwrap_or_default()
                .to_be_bytes()
                .as_ref(),
        );
        if let Some(data) = self.data {
            buf.extend_from_slice(&data.as_slice());
        }
        buf
    }
}

impl<'d, D: Disk> TreeEntry<'d, D> {
    fn tree(&self) -> BTree {
        BTree::from_offset(self.offset)
    }
    fn insert_child_tree(&mut self, key: Key) -> io::Result<BTree> {
        let child = BTree::init(self.db)?;
        let mut tree = self.tree();
        let existing_value = tree.lookup(key, self.db)?;
        let mut entry = match existing_value {
            Some(data) => TreeEntryValue::from_data(data),
            None => TreeEntryValue::new(),
        };

        entry.child_offset = std::num::NonZeroU64::new(child.offset());
        tree.insert(key, &entry.into_buf(), self.db)?;
        Ok(child)
    }
    pub fn get(mut self, key: Key) -> io::Result<Self> {
        let tree = self.tree();
        let offset = match tree.lookup(key, self.db)? {
            Some(buf) => match TreeEntryValue::from_data(buf).child_offset {
                Some(offset) => offset.get(),
                None => self.insert_child_tree(key)?.offset(),
            },
            None => self.insert_child_tree(key)?.offset(),
        };
        Ok(TreeEntry {
            db: self.db,
            offset,
        })
    }
    pub fn set_value(self, key: Key, data: &[u8]) -> io::Result<()> {
        let mut tree = BTree::from_offset(self.offset);
        let mut entry = match tree.lookup(key, self.db)? {
            Some(data) => TreeEntryValue::from_data(data),
            None => TreeEntryValue::new(),
        };
        entry.data = Some(data.to_vec());
        tree.insert(key, &entry.into_buf(), self.db)
    }
    pub fn value(self, key: Key) -> io::Result<Option<Vec<u8>>> {
        let tree = BTree::from_offset(self.offset);
        Ok(tree
            .lookup(key, self.db)?
            .and_then(|data| TreeEntryValue::from_data(data).data))
    }
}

#[test]
fn test_tree() -> io::Result<()> {
    use std::io::Cursor;
    let mut db = Database::initialize(Cursor::new(vec![]))?;
    const USERS: u128 = 10;
    const USERNAME: u128 = 40;
    let expected_value = &[1, 2, 3, 4];

    let user_id = 40;
    db.get(USERS)?
        .get(user_id)?
        .set_value(USERNAME, expected_value)?;
    let value = db.get(USERS)?.get(user_id)?.value(USERNAME)?.unwrap();
    assert_eq!(value.as_slice(), expected_value);
    Ok(())
}

#[test]
fn cannot_mix_children_and_values() -> io::Result<()> {
    use std::io::Cursor;
    let mut db = Database::initialize(Cursor::new(vec![]))?;
    const USERS: u128 = 10;
    const USERNAME: u128 = 40;
    let all_user_buf = &[1, 2, 3, 4];
    let username_buf = &[6, 7, 8];

    let user_id = 40;
    db.get(USERS)?.set_value(user_id, all_user_buf)?;
    db.get(USERS)?
        .get(user_id)?
        .set_value(USERNAME, username_buf)?;

    assert_eq!(
        db.get(USERS)?.value(user_id)?.unwrap().as_slice(),
        all_user_buf
    );

    assert_eq!(
        db.get(USERS)?
            .get(user_id)?
            .value(USERNAME)?
            .unwrap()
            .as_slice(),
        username_buf
    );

    Ok(())
}
