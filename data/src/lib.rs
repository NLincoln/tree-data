mod database;
mod page;
mod tree;

pub type Key = u128;

use database::BlockAllocator;
pub use database::Database;
pub use database::Disk;
pub use page::BTree;
