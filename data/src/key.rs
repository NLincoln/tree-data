use uuid::Uuid;

pub enum Key {
    String(String),
    Bytes(Vec<u8>),
    Uuid(Uuid),
    I64(i64),
}
