use criterion::{criterion_group, criterion_main, Criterion};
use data::{BTree, Database, Disk};
use std::fs::OpenOptions;

fn btree() -> (Database<impl Disk>, BTree) {
    let disk = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .read(true)
        .open("insertion-bench")
        .unwrap();
    let mut db = Database::initialize(disk).unwrap();
    let tree = BTree::init(&mut db).unwrap();
    (db, tree)
}

fn btree_insert_n(n: u128) {
    let (mut db, mut tree) = btree();
    for key in 0..n {
        tree.insert(key, &[0, 1, 2, 3, 4], &mut db).unwrap();
    }
}

fn btree_read_n(n: u128) {
    let (mut db, mut tree) = btree();
    for key in 0..20 {
        tree.insert(key, &[0, 1, 2, 3, 4], &mut db).unwrap();
    }
    for key in 0..n {
        tree.lookup(key % 20, &mut db).unwrap();
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
    c.bench_function("btree_read 100", |b| {
        b.iter(|| btree_read_n(100));
    });
    c.bench_function("btree_read 1000", |b| {
        b.iter(|| btree_read_n(1000));
    });
    // c.bench_function("btree_insert 10,000", |b| {
    //     b.iter(|| btree_insert_n(10_000));
    // });
    //    c.bench_function("btree_insert 100,000", |b| {
    //        b.iter(|| btree_insert_n(100_000));
    //    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
