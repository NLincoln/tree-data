mod database;
mod key;
mod page;
mod tree;

use key::Key;

use database::BlockAllocator;
pub use database::Database;
pub use database::Disk;
pub use page::BTree;
