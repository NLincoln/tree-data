use criterion::{criterion_group, criterion_main, Criterion};
use data::{BTree, Database, Disk};

fn btree() -> (Database<impl Disk>, BTree) {
    use std::io::Cursor;
    let mut db = Database::initialize(Cursor::new(vec![])).unwrap();
    let tree = BTree::init(&mut db).unwrap();
    (db, tree)
}

fn btree_insert_n(n: u128) {
    let (mut db, mut tree) = btree();
    for key in 0..n {
        tree.insert(key, &[0, 1, 2, 3, 4], &mut db).unwrap();
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("btree_insert 10", |b| {
        b.iter(|| btree_insert_n(10));
    });
    c.bench_function("btree_insert 100", |b| {
        b.iter(|| btree_insert_n(100));
    });
    c.bench_function("btree_insert 1,000", |b| {
        b.iter(|| btree_insert_n(1_000));
    });
    c.bench_function("btree_insert 10,000", |b| {
        b.iter(|| btree_insert_n(10_000));
    });
    //    c.bench_function("btree_insert 100,000", |b| {
    //        b.iter(|| btree_insert_n(100_000));
    //    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
