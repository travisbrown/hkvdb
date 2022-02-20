use super::{
    error::Error,
    value::{Set64, Value},
};
use rocksdb::{
    BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, DataBlockIndexType, IteratorMode,
    MergeOperands, Options, SliceTransform, WriteBatch, DB,
};
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CaseSensitivity {
    Sensitive,
    Insensitive,
}

#[derive(Clone)]
pub struct Hkvdb<V> {
    db: Arc<DB>,
    options: Options,
    _merge: PhantomData<V>,
}

impl<V: Value + 'static> Hkvdb<V> {
    pub fn new<P: AsRef<Path>>(path: P, enable_statistics: bool) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_missing_column_families(true);
        options.create_if_missing(true);

        if enable_statistics {
            options.enable_statistics();
        }

        let mut by_id_cf_block_options = BlockBasedOptions::default();
        by_id_cf_block_options.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
        by_id_cf_block_options.set_block_cache(&rocksdb::Cache::new_lru_cache(32768 * 2)?);

        let mut by_id_cf_options = Options::default();
        by_id_cf_options.set_block_based_table_factory(&by_id_cf_block_options);
        by_id_cf_options.set_merge_operator_associative("merge_by_id", Self::merge_by_id);
        by_id_cf_options.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));

        let mut index_cf_block_options = BlockBasedOptions::default();
        index_cf_block_options.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);

        let mut index_cf_options = Options::default();
        index_cf_options.set_block_based_table_factory(&index_cf_block_options);
        index_cf_options.set_merge_operator_associative("merge_index", Self::merge_index);

        let by_id_cf = ColumnFamilyDescriptor::new("by_id", by_id_cf_options);
        let index_cf = ColumnFamilyDescriptor::new("index", index_cf_options);

        let db = DB::open_cf_descriptors(&options, path, vec![by_id_cf, index_cf])?;

        Ok(Self {
            db: Arc::new(db),
            options,
            _merge: PhantomData,
        })
    }

    pub fn statistics(&self) -> Option<String> {
        self.options.get_statistics()
    }

    fn by_id_cf(&self) -> &ColumnFamily {
        self.db.cf_handle("by_id").unwrap()
    }

    fn index_cf(&self) -> &ColumnFamily {
        self.db.cf_handle("index").unwrap()
    }

    pub fn get_estimated_key_count(&self) -> Result<u64, Error> {
        Ok(self
            .db
            .property_int_value("rocksdb.estimate-num-keys")?
            .unwrap())
    }

    pub fn get_counts(&self) -> Result<(u64, u64), Error> {
        let mut ids = HashSet::new();
        let mut value_count = 0;

        let iter = self.db.iterator_cf(self.by_id_cf(), IteratorMode::Start);

        for (key, _) in iter {
            let id = u64::from_be_bytes(
                key[0..8]
                    .try_into()
                    .map_err(|_| Error::InvalidKey(key.to_vec()))?,
            );

            ids.insert(id);
            value_count += 1;
        }

        Ok((ids.len() as u64, value_count))
    }

    pub fn get_raw(&self, id: u64) -> Result<HashMap<Vec<u8>, V>, Error> {
        let prefix = Self::make_prefix(id);
        let mut result = HashMap::new();
        let iterator = self.db.prefix_iterator_cf(self.by_id_cf(), prefix);

        for (key, value_bytes) in iterator {
            let next_id = u64::from_be_bytes(
                key[0..8]
                    .try_into()
                    .map_err(|_| Error::InvalidKey(key.to_vec()))?,
            );

            if next_id == id {
                let value = V::prepare(&value_bytes)?;
                result.insert(key[8..].to_vec(), value);
            } else {
                break;
            }
        }

        Ok(result)
    }

    pub fn get(&self, id: u64) -> Result<HashMap<String, V>, Error> {
        let as_bytes = self.get_raw(id)?;
        let mut result = HashMap::with_capacity(as_bytes.len());

        for (k, v) in as_bytes {
            result.insert(String::from_utf8(k).map_err(|error| error.utf8_error())?, v);
        }

        Ok(result)
    }

    pub fn iter_raw(&self) -> impl Iterator<Item = Result<(u64, Vec<u8>, V), Error>> + '_ {
        self.db
            .iterator_cf(self.by_id_cf(), IteratorMode::Start)
            .map(|(key, value_bytes)| {
                let id = u64::from_be_bytes(
                    key[0..8]
                        .try_into()
                        .map_err(|_| Error::InvalidKey(key.to_vec()))?,
                );

                let value = V::prepare(&value_bytes)?;

                Ok((id, key[8..].to_vec(), value))
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<(u64, String, V), Error>> + '_ {
        self.iter_raw().map(|result| {
            result.and_then(|(id, bytes, value)| {
                Ok((
                    id,
                    String::from_utf8(bytes).map_err(|error| error.utf8_error())?,
                    value,
                ))
            })
        })
    }

    pub fn put_raw<IV: Into<V>>(&self, id: u64, data: &[u8], value: IV) -> Result<(), Error> {
        let key = Self::make_key(id, data);
        self.db
            .merge_cf(self.by_id_cf(), key, value.into().into())?;
        Ok(())
    }

    pub fn put_raw_batch<'a, IV: Into<V>, I: IntoIterator<Item = (u64, &'a [u8], IV)>>(
        &'a self,
        batch: I,
    ) -> Result<(), Error> {
        let cf = self.by_id_cf();
        let mut wb = WriteBatch::default();

        for (id, data, value) in batch {
            let key = Self::make_key(id, data);
            wb.merge_cf(cf, key, value.into().into());
        }

        Ok(self.db.write(wb)?)
    }

    pub fn put<IV: Into<V>>(&self, id: u64, data: &str, value: IV) -> Result<(), Error> {
        self.put_raw(id, data.as_bytes(), value)
    }

    pub fn put_batch<S: AsRef<str>, IV: Into<V>, I: IntoIterator<Item = (u64, S, IV)>>(
        &self,
        batch: I,
    ) -> Result<(), Error> {
        let cf = self.by_id_cf();
        let mut wb = WriteBatch::default();

        for (id, data, value) in batch {
            let key = Self::make_key(id, data.as_ref().as_bytes());
            wb.merge_cf(cf, key, value.into().into());
        }

        Ok(self.db.write(wb)?)
    }

    fn make_prefix(id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(8);
        key.extend_from_slice(&id.to_be_bytes());
        key
    }

    fn make_key(id: u64, value: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(value.len() + 8);
        key.extend_from_slice(&id.to_be_bytes());
        key.extend_from_slice(value);
        key
    }

    pub fn search_raw(
        &self,
        data: &[u8],
        case_sensitivity: CaseSensitivity,
    ) -> Result<Vec<u64>, Error> {
        let key = Self::make_index_key(data, case_sensitivity)?;

        match self.db.get_pinned_cf(self.index_cf(), key)? {
            Some(bytes) => Ok(Set64::try_from(bytes.as_ref())?.into_inner()),
            None => Ok(vec![]),
        }
    }

    pub fn search(&self, data: &str) -> Result<Vec<u64>, Error> {
        self.search_raw(data.as_bytes(), CaseSensitivity::Sensitive)
    }

    pub fn search_ci(&self, data: &str) -> Result<Vec<u64>, Error> {
        self.search_raw(data.to_lowercase().as_bytes(), CaseSensitivity::Insensitive)
    }

    pub fn make_index(&self, case_sensitivity: CaseSensitivity) -> Result<(), Error> {
        let iter = self.db.iterator_cf(self.by_id_cf(), IteratorMode::Start);

        for (id_data_key, _) in iter {
            let id = u64::from_be_bytes(
                id_data_key[0..8]
                    .try_into()
                    .map_err(|_| Error::InvalidKey(id_data_key.to_vec()))?,
            );

            let index_key = Self::make_index_key(&id_data_key[8..], case_sensitivity)?;
            let id_bytes: Vec<u8> = Set64::singleton(id).into();

            self.db.merge_cf(self.index_cf(), &index_key, &id_bytes)?;
        }

        Ok(())
    }

    pub fn make_index_key(
        data: &[u8],
        case_sensitivity: CaseSensitivity,
    ) -> Result<Vec<u8>, Error> {
        let mut key = Vec::with_capacity(data.len());

        if case_sensitivity == CaseSensitivity::Insensitive {
            let as_string = std::str::from_utf8(data)?;
            let lowercase = as_string.to_lowercase();

            key.extend(lowercase.as_bytes());
        } else {
            key.extend_from_slice(data);
        }

        Ok(key)
    }

    fn merge_by_id(
        _key: &[u8],
        existing_value: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        V::merge(existing_value, operands.iter()).unwrap_or_else(|(error, fallback_value)| {
            // The RocksDb library doesn't let us fail in a merge, so we just log the
            // error and use the last value before the error. This should never happen.
            log::error!("Error during aggregation in merge: {:?}", error);

            fallback_value
        })
    }

    fn merge_index(
        _key: &[u8],
        existing_value: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        Set64::merge(existing_value, operands.iter()).unwrap_or_else(|(error, fallback_value)| {
            // The RocksDb library doesn't let us fail in a merge, so we just log the
            // error and use the last value before the error. This should never happen.
            log::error!("Error during aggregation in index merge: {:?}", error);

            fallback_value
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::value::{Range32, Set32};
    use super::*;

    struct Observation {
        id: u64,
        value: String,
        timestamp: u32,
    }

    impl Observation {
        fn new(id: u64, value: &str, timestamp: u32) -> Self {
            Self {
                id,
                value: value.to_string(),
                timestamp,
            }
        }
    }

    fn observations() -> Vec<Observation> {
        vec![
            Observation::new(1, "foo", 101),
            Observation::new(1, "bar", 1),
            Observation::new(1, "foo", 23),
            Observation::new(2, "FOO", 23),
            Observation::new(1, "qux", 50),
            Observation::new(1, "bar", 1),
            Observation::new(1, "qux", 0),
            Observation::new(2, "abc", 23),
        ]
    }

    #[test]
    fn get_counts() {
        let dir = tempfile::tempdir().unwrap();
        let db: Hkvdb<Range32> = Hkvdb::new(dir, false).unwrap();

        for observation in observations() {
            db.put(observation.id, &observation.value, observation.timestamp)
                .unwrap();
        }

        assert_eq!(db.get_counts().unwrap(), (2, 5));
    }

    #[test]
    fn put_raw_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db: Hkvdb<Range32> = Hkvdb::new(dir, false).unwrap();

        db.put_raw_batch(observations().iter().map(|observation| {
            (
                observation.id,
                observation.value.as_bytes(),
                observation.timestamp,
            )
        }))
        .unwrap();

        let expected = vec![
            ("foo".to_string(), (23, 101).into()),
            ("bar".to_string(), (1, 1).into()),
            ("qux".to_string(), (0, 50).into()),
        ]
        .into_iter()
        .collect();

        assert_eq!(db.get(1).unwrap(), expected);
    }

    #[test]
    fn put_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db: Hkvdb<Range32> = Hkvdb::new(dir, false).unwrap();

        db.put_batch(
            observations()
                .iter()
                .map(|observation| (observation.id, &observation.value, observation.timestamp)),
        )
        .unwrap();

        let expected = vec![
            ("foo".to_string(), (23, 101).into()),
            ("bar".to_string(), (1, 1).into()),
            ("qux".to_string(), (0, 50).into()),
        ]
        .into_iter()
        .collect();

        assert_eq!(db.get(1).unwrap(), expected);
    }

    #[test]
    fn iter() {
        let dir = tempfile::tempdir().unwrap();
        let db: Hkvdb<Range32> = Hkvdb::new(dir, false).unwrap();

        db.put_batch(
            observations()
                .iter()
                .map(|observation| (observation.id, &observation.value, observation.timestamp)),
        )
        .unwrap();

        let expected: Vec<(u64, String, Range32)> = vec![
            (1, "bar".to_string(), (1, 1).into()),
            (1, "foo".to_string(), (23, 101).into()),
            (1, "qux".to_string(), (0, 50).into()),
            (2, "FOO".to_string(), (23, 23).into()),
            (2, "abc".to_string(), (23, 23).into()),
        ]
        .into_iter()
        .collect();

        assert_eq!(db.iter().collect::<Result<Vec<_>, _>>().unwrap(), expected);
    }

    #[test]
    fn timestamp_range() {
        let dir = tempfile::tempdir().unwrap();
        let db: Hkvdb<Range32> = Hkvdb::new(dir, false).unwrap();

        for observation in observations() {
            db.put(observation.id, &observation.value, observation.timestamp)
                .unwrap();
        }

        let expected = vec![
            ("foo".to_string(), (23, 101).into()),
            ("bar".to_string(), (1, 1).into()),
            ("qux".to_string(), (0, 50).into()),
        ]
        .into_iter()
        .collect();

        assert_eq!(db.get(1).unwrap(), expected);
    }

    #[test]
    fn timestamp_set() {
        let dir = tempfile::tempdir().unwrap();
        let db: Hkvdb<Set32> = Hkvdb::new(dir, false).unwrap();

        for observation in observations() {
            db.put(observation.id, &observation.value, observation.timestamp)
                .unwrap();
        }

        let expected = vec![
            ("foo".to_string(), Set32::new(&[23, 101])),
            ("bar".to_string(), Set32::new(&[1])),
            ("qux".to_string(), Set32::new(&[0, 50])),
        ]
        .into_iter()
        .collect();

        assert_eq!(db.get(1).unwrap(), expected);
    }

    #[test]
    fn search() {
        let dir = tempfile::tempdir().unwrap();
        let db: Hkvdb<Set32> = Hkvdb::new(dir, false).unwrap();

        for observation in observations() {
            db.put(observation.id, &observation.value, observation.timestamp)
                .unwrap();
        }

        db.make_index(CaseSensitivity::Sensitive).unwrap();

        assert_eq!(db.search("foo").unwrap(), vec![1]);
    }

    #[test]
    fn search_ci() {
        let dir = tempfile::tempdir().unwrap();
        let db: Hkvdb<Set32> = Hkvdb::new(dir, false).unwrap();

        for observation in observations() {
            db.put(observation.id, &observation.value, observation.timestamp)
                .unwrap();
        }

        db.make_index(CaseSensitivity::Insensitive).unwrap();

        assert_eq!(db.search_ci("foo").unwrap(), vec![1, 2]);
    }

    #[test]
    fn demo_test() {
        demo().unwrap();
    }

    fn demo() -> Result<(), super::super::Error> {
        struct UserSnapshot {
            user_id: u64,
            timestamp_s: u32,
            screen_name: String,
        }

        let snapshots = vec![
            UserSnapshot {
                user_id: 770781940341288960,
                timestamp_s: 1577933499,
                screen_name: "RudyGiuliani".to_string(),
            },
            UserSnapshot {
                user_id: 770781940341288960,
                timestamp_s: 1479920042,
                screen_name: "xxxxxxx37583982".to_string(),
            },
            UserSnapshot {
                user_id: 6510972,
                timestamp_s: 1643648042,
                screen_name: "travisbrown".to_string(),
            },
            // Millions of other user profile snapshots?
        ];

        let dir = tempfile::tempdir().unwrap();
        let db: Hkvdb<Range32> = Hkvdb::new(dir, false).unwrap();

        for snapshot in snapshots {
            db.put(
                snapshot.user_id,
                &snapshot.screen_name,
                snapshot.timestamp_s,
            )?;
        }

        let values = db.get(770781940341288960)?;

        let mut expected = HashMap::new();
        expected.insert("xxxxxxx37583982".to_string(), 1479920042.into());
        expected.insert("RudyGiuliani".to_string(), 1577933499.into());

        assert_eq!(values, expected);

        db.make_index(CaseSensitivity::Insensitive)?;

        let user_ids = db.search_ci("RuDYgiuLianI")?;

        assert_eq!(user_ids, vec![770781940341288960]);

        Ok(())
    }
}
