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

impl<'d, D: Disk> TreeEntry<'d, D> {
    const OFFSET_TAG: u8 = 1;
    const VALUE_TAG: u8 = 2;
    pub fn get(self, key: Key) -> io::Result<Self> {
        let mut tree = BTree::from_offset(self.offset);
        let offset = match tree.lookup(key, self.db)? {
            Some(buf) => {
                assert_eq!(buf[0], Self::OFFSET_TAG);
                read_be_u64(&buf[1..])
            }
            None => {
                let child_tree = BTree::init(self.db)?;
                let mut data = Vec::with_capacity(std::mem::size_of::<u64>() + 1);
                data.push(Self::OFFSET_TAG);
                data.extend_from_slice(&child_tree.offset().to_be_bytes());
                tree.insert(key, &data, self.db)?;
                child_tree.offset()
            }
        };
        Ok(TreeEntry {
            db: self.db,
            offset,
        })
    }
    pub fn set_value(self, key: Key, data: &[u8]) -> io::Result<()> {
        let mut tree = BTree::from_offset(self.offset);
        let mut buf = Vec::with_capacity(data.len() + 1);
        buf.push(Self::VALUE_TAG);
        buf.extend_from_slice(data);
        tree.insert(key, &buf[..], self.db)
    }
    pub fn value(self, key: Key) -> io::Result<Option<Vec<u8>>> {
        let tree = BTree::from_offset(self.offset);
        Ok(tree.lookup(key, self.db)?.map(|mut data| {
            assert_eq!(data[0], Self::VALUE_TAG);
            data.remove(0);
            data
        }))
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
#[should_panic]
fn cannot_mix_children_and_values() {
    use std::io::Cursor;
    let mut db = Database::initialize(Cursor::new(vec![])).unwrap();
    const USERS: u128 = 10;
    const USERNAME: u128 = 40;
    let expected_value = &[1, 2, 3, 4];

    let user_id = 40;
    db.get(USERS)
        .unwrap()
        .set_value(user_id, expected_value)
        .unwrap();
    db.get(USERS)
        .unwrap()
        .get(user_id)
        .unwrap()
        .get(USERNAME)
        .unwrap();
}
