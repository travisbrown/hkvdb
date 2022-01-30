use super::error::Error;
use std::ops::Add;

type MaybeBytes = Option<Vec<u8>>;

/// A convenience trait that bundles up the operations needed for values.
pub trait Value: Add<Output = Self> + Into<Vec<u8>> + Sized {
    /// This is a hack because I couldn't figure out how to just use `TryFrom` directly.
    fn prepare(bytes: &[u8]) -> Result<Self, Error>;

    fn merge<'a, I: Iterator<Item = &'a [u8]>>(
        existing: Option<&[u8]>,
        new_values: I,
    ) -> Result<MaybeBytes, (Error, MaybeBytes)> {
        let mut aggregated = match existing.map_or(Ok(None), |value| Self::prepare(value).map(Some))
        {
            Ok(value) => value,
            Err(error) => {
                return Err((error, new_values.last().map(|last| last.to_vec())));
            }
        };

        for bytes in new_values {
            let prepared = match Self::prepare(bytes) {
                Ok(value) => value,
                Err(error) => {
                    return Err((error, aggregated.map(Self::into)));
                }
            };

            aggregated = match aggregated {
                Some(current) => Some(current + prepared),
                None => Some(prepared),
            };
        }

        Ok(aggregated.map(Self::into))
    }
}

/// Represents a time range.
///
/// The values will generally be epoch seconds, but this isn't necessary.
#[derive(Debug, Eq, PartialEq)]
pub struct Range32 {
    first: u32,
    last: u32,
}

impl Range32 {
    pub fn new(first: u32, last: u32) -> Self {
        Self { first, last }
    }

    pub fn singleton(value: u32) -> Self {
        Self::new(value, value)
    }

    pub fn first(&self) -> u32 {
        self.first
    }

    pub fn last(&self) -> u32 {
        self.last
    }
}

impl From<(u32, u32)> for Range32 {
    fn from(input: (u32, u32)) -> Self {
        Self::new(input.0, input.1)
    }
}

impl From<u32> for Range32 {
    fn from(input: u32) -> Self {
        Self::singleton(input)
    }
}

impl Add for Range32 {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        Self::new(self.first.min(other.first), self.last.max(other.last))
    }
}

impl From<Range32> for Vec<u8> {
    fn from(input: Range32) -> Self {
        let mut result = Vec::with_capacity(8);
        result.extend_from_slice(&input.first.to_be_bytes());
        result.extend_from_slice(&input.last.to_be_bytes());
        result
    }
}

impl TryFrom<&[u8]> for Range32 {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() == 8 {
            let first = u32::from_be_bytes(
                bytes[0..4]
                    .try_into()
                    .map_err(|_| Error::invalid_value(bytes))?,
            );
            let last = u32::from_be_bytes(
                bytes[4..8]
                    .try_into()
                    .map_err(|_| Error::invalid_value(bytes))?,
            );

            Ok(Self { first, last })
        } else {
            Err(Error::invalid_value(bytes))
        }
    }
}

impl Value for Range32 {
    fn prepare(bytes: &[u8]) -> Result<Self, Error> {
        Self::try_from(bytes)
    }
}

/// Represents a set of time observations as a sorted, deduplicated sequence.
///
/// The values will generally be epoch seconds, but this isn't necessary.
#[derive(Debug, Eq, PartialEq)]
pub struct Set32 {
    values: Vec<u32>,
}

impl Set32 {
    pub fn new(values: &[u32]) -> Self {
        let mut values = values.to_vec();
        values.sort_unstable();
        values.dedup();
        Self { values }
    }

    pub fn singleton(value: u32) -> Self {
        Self::new(&[value])
    }

    pub fn values(&self) -> &[u32] {
        &self.values
    }

    pub fn into_inner(self) -> Vec<u32> {
        self.values
    }
}

impl From<&[u32]> for Set32 {
    fn from(input: &[u32]) -> Self {
        Self::new(input)
    }
}

impl From<u32> for Set32 {
    fn from(input: u32) -> Self {
        Self::singleton(input)
    }
}

impl Add for Set32 {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        let mut values = Vec::with_capacity(self.values.len() + other.values.len());
        values.extend(self.values);
        values.extend(other.values);
        values.sort_unstable();
        values.dedup();
        Self { values }
    }
}

impl From<Set32> for Vec<u8> {
    fn from(input: Set32) -> Self {
        let mut result = Vec::with_capacity(4 * input.values.len());
        for value in input.values {
            result.extend_from_slice(&value.to_be_bytes());
        }
        result
    }
}

impl TryFrom<&[u8]> for Set32 {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() % 4 == 0 {
            let len = bytes.len() / 4;
            let mut result = Vec::with_capacity(len);

            for i in 0..len {
                let value = u32::from_be_bytes(
                    bytes[i * 4..i * 4 + 4]
                        .try_into()
                        .map_err(|_| Error::invalid_value(bytes))?,
                );

                result.push(value);
            }

            Ok(Self { values: result })
        } else {
            Err(Error::invalid_value(bytes))
        }
    }
}

impl Value for Set32 {
    fn prepare(bytes: &[u8]) -> Result<Self, Error> {
        Self::try_from(bytes)
    }
}
/// Represents a set of unsigned integers.
#[derive(Debug, Eq, PartialEq)]
pub struct Set64 {
    values: Vec<u64>,
}

impl Set64 {
    pub fn new(values: &[u64]) -> Self {
        let mut values = values.to_vec();
        values.sort_unstable();
        values.dedup();
        Self { values }
    }

    pub fn singleton(value: u64) -> Self {
        Self::new(&[value])
    }

    pub fn values(&self) -> &[u64] {
        &self.values
    }

    pub fn into_inner(self) -> Vec<u64> {
        self.values
    }
}

impl From<&[u64]> for Set64 {
    fn from(input: &[u64]) -> Self {
        Self::new(input)
    }
}

impl From<u64> for Set64 {
    fn from(input: u64) -> Self {
        Self::singleton(input)
    }
}

impl Add for Set64 {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        let mut values = Vec::with_capacity(self.values.len() + other.values.len());
        values.extend(self.values);
        values.extend(other.values);
        values.sort_unstable();
        values.dedup();
        Self { values }
    }
}

impl From<Set64> for Vec<u8> {
    fn from(input: Set64) -> Self {
        let mut result = Vec::with_capacity(8 * input.values.len());
        for value in input.values {
            result.extend_from_slice(&value.to_be_bytes());
        }
        result
    }
}

impl TryFrom<&[u8]> for Set64 {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() % 8 == 0 {
            let len = bytes.len() / 8;
            let mut result = Vec::with_capacity(len);

            for i in 0..len {
                let value = u64::from_be_bytes(
                    bytes[i * 8..i * 8 + 8]
                        .try_into()
                        .map_err(|_| Error::invalid_value(bytes))?,
                );

                result.push(value);
            }

            Ok(Self { values: result })
        } else {
            Err(Error::invalid_value(bytes))
        }
    }
}

impl Value for Set64 {
    fn prepare(bytes: &[u8]) -> Result<Self, Error> {
        Self::try_from(bytes)
    }
}
