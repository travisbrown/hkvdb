#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("RocksDb error")]
    Db(#[from] rocksdb::Error),
    #[error("Invalid key")]
    InvalidKey(Vec<u8>),
    #[error("Invalid value")]
    InvalidValue(Vec<u8>),
    #[error("Invalid UTF-8")]
    InvalidUtf8(#[from] std::str::Utf8Error),
}

impl Error {
    pub fn invalid_value(value: &[u8]) -> Self {
        Self::InvalidValue(value.to_vec())
    }
}
