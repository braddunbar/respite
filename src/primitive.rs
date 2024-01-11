use crate::{RespError, RespValue};
use bytes::Bytes;

/// A primitive value that can be used as the key for a map or set.
#[derive(Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RespPrimitive {
    Integer(i64),
    Nil,
    String(Bytes),
}

impl From<i64> for RespPrimitive {
    fn from(value: i64) -> Self {
        RespPrimitive::Integer(value)
    }
}

impl From<&'static str> for RespPrimitive {
    fn from(value: &'static str) -> Self {
        RespPrimitive::String(value.into())
    }
}

impl From<String> for RespPrimitive {
    fn from(value: String) -> Self {
        RespPrimitive::String(value.into())
    }
}

impl TryFrom<RespValue> for RespPrimitive {
    type Error = RespError;

    fn try_from(value: RespValue) -> Result<Self, Self::Error> {
        match value {
            RespValue::Integer(value) => Ok(RespPrimitive::Integer(value)),
            RespValue::Nil => Ok(RespPrimitive::Nil),
            RespValue::String(value) => Ok(RespPrimitive::String(value)),
            _ => Err(RespError::RespPrimitive),
        }
    }
}
