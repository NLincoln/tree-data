use data::{BTree, Database, Disk};
use rand::Rng;
use std::{
    collections::HashMap,
    io::{self, Write},
};

type Key = u128;
type Data = Vec<u8>;

#[derive(Debug, Clone)]
enum Instruction {
    Insert(Key, Vec<u8>),
    Delete(Key),
}

fn validate(
    reference: &HashMap<Key, Data>,
    btree: &mut BTree,
    db: &mut Database<impl Disk>,
) -> io::Result<bool> {
    for (&key, value) in reference.iter() {
        if value != &btree.lookup(key, db)?.unwrap() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn generate_instruction(reference: &HashMap<Key, Data>) -> Instruction {
    use rand::{
        distributions::{Distribution, Uniform},
        seq::IteratorRandom,
    };
    let mut rng = rand::thread_rng();
    if rng.gen_bool(0.8) {
        let key = if rng.gen_bool(0.3) {
            match reference.keys().choose(&mut rng) {
                Some(val) => *val,
                None => return generate_instruction(reference),
            }
        } else {
            Uniform::from(1..1_000_000_000_000_000).sample(&mut rng)
        };
        let data_len: usize = Uniform::from(0..20).sample(&mut rng);
        let data: Vec<u8> = Uniform::from(0..128)
            .sample_iter(&mut rng)
            .take(data_len)
            .collect();
        Instruction::Insert(key, data)
    } else {
        let key = if rng.gen_bool(0.9) {
            match reference.keys().choose(&mut rng) {
                Some(val) => *val,
                None => return generate_instruction(reference),
            }
        } else {
            Uniform::from(1..1_000_000_000_000_000).sample(&mut rng)
        };
        Instruction::Delete(key)
    }
}

fn main() -> io::Result<()> {
    use std::io::Cursor;
    let mut db = Database::initialize(Cursor::new(vec![])).unwrap();
    let mut tree = BTree::init(&mut db).unwrap();
    let mut reference = HashMap::new();
    let mut instructions = vec![];
    let mut file = std::fs::File::create("instructions")?;
    loop {
        let instruction = generate_instruction(&reference);
        match &instruction {
            Instruction::Delete(key) => {
                tree.delete(*key, &mut db)?;
                reference.remove(&key);
            }
            Instruction::Insert(key, data) => {
                tree.insert(*key, data, &mut db)?;
                reference.insert(*key, data.clone());
            }
        }
        instructions.push(instruction);
        if !validate(&reference, &mut tree, &mut db)? {
            for inst in instructions {
                match inst {
                    Instruction::Insert(key, value) => {
                        writeln!(file, "INSERT {} {:?}", key, value)?;
                    }
                    Instruction::Delete(key) => {
                        writeln!(file, "DELETE {}", key)?;
                    }
                }
            }
            break;
        }
    }
    Ok(())
}
