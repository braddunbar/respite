use crate::RespPrimitive;
use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, BTreeSet};

/// A RESP value, possibly built from many frames.
///
/// These values are meant to be used for testing, and thus can be hashed and compared. However,
/// this also makes them far less performant than reading frames directly.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RespValue {
    Attribute(BTreeMap<RespPrimitive, RespValue>),
    Array(Vec<RespValue>),
    Bignum(Bytes),
    Boolean(bool),
    Double(OrderedFloat<f64>),
    Error(Bytes),
    Integer(i64),
    Map(BTreeMap<RespPrimitive, RespValue>),
    Nil,
    Push(Vec<RespValue>),
    Set(BTreeSet<RespPrimitive>),
    String(Bytes),
    Verbatim(Bytes, Bytes),
}

impl From<bool> for RespValue {
    fn from(value: bool) -> Self {
        RespValue::Boolean(value)
    }
}

impl From<i32> for RespValue {
    fn from(value: i32) -> Self {
        RespValue::Integer(value.into())
    }
}

impl From<i64> for RespValue {
    fn from(value: i64) -> Self {
        RespValue::Integer(value)
    }
}

impl From<f64> for RespValue {
    fn from(value: f64) -> Self {
        RespValue::Double(value.into())
    }
}

impl From<String> for RespValue {
    fn from(value: String) -> Self {
        RespValue::String(value.into())
    }
}

impl From<&'static str> for RespValue {
    fn from(value: &'static str) -> Self {
        RespValue::String(value.into())
    }
}

impl<const N: usize> From<&'static [u8; N]> for RespValue {
    fn from(value: &'static [u8; N]) -> Self {
        RespValue::String((&value[..]).into())
    }
}

impl From<Vec<u8>> for RespValue {
    fn from(value: Vec<u8>) -> Self {
        RespValue::String(value.into())
    }
}

impl RespValue {
    /// Extract a [`Vec`] of values, if this value is an array.
    pub fn array(&mut self) -> Option<&mut Vec<RespValue>> {
        if let RespValue::Array(value) = self {
            Some(value)
        } else {
            None
        }
    }

    /// Extract an error message if this value is an error.
    pub fn error(&self) -> Option<&str> {
        if let RespValue::Error(value) = self {
            std::str::from_utf8(value).ok()
        } else {
            None
        }
    }

    /// Extract an [`i64`] if this value is an integer.
    pub fn integer(&self) -> Option<i64> {
        if let RespValue::Integer(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    /// Extract the text value of this value if it has one.
    pub fn text(&self) -> Option<&str> {
        use RespValue::*;

        if let String(text) | Verbatim(_, text) = self {
            std::str::from_utf8(text).ok()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array() {
        assert_eq!(RespValue::Array(vec![]), resp! { [] });
        assert_eq!(
            RespValue::Array(vec![RespValue::Array(vec![])]),
            resp! { [[]] }
        );
        assert_eq!(RespValue::Array(vec![RespValue::Nil]), resp! { [nil] });
        assert_eq!(RespValue::Array(vec![1i64.into()]), resp! { [1i64] });
        assert_eq!(
            RespValue::Array(vec![1i64.into(), 2i64.into()]),
            resp! { [1i64, 2i64] }
        );
    }

    #[test]
    fn bignum() {
        assert_eq!(RespValue::Bignum("1234".into()), resp! { (big "1234") });
        assert_eq!(
            RespValue::Array(vec![RespValue::Bignum("1234".into())]),
            resp! { [(big "1234")] }
        );
    }

    #[test]
    fn boolean() {
        assert_eq!(RespValue::Boolean(true), resp! { true });
        assert_eq!(RespValue::Boolean(false), resp! { false });
        assert_eq!(
            RespValue::Array(vec![true.into(), false.into()]),
            resp! { [true, false] }
        );
    }

    #[test]
    fn string() {
        assert_eq!(RespValue::String("1234".into()), resp! { "1234" });
    }

    #[test]
    fn double() {
        assert_eq!(RespValue::Double(1f64.into()), resp! { 1f64 });
    }

    #[test]
    fn integer() {
        assert_eq!(RespValue::Integer(1i64), resp! { 1i64 });
        assert_eq!(RespValue::Integer(-1i64), resp! { (-1) });
    }

    #[test]
    fn map() {
        // Bytes is a false positive here.
        // <https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type>
        #[allow(clippy::mutable_key_type)]
        let mut map = BTreeMap::new();
        map.insert("x".into(), "1".into());
        map.insert(1i64.into(), 1f64.into());
        map.insert(RespPrimitive::Nil, RespValue::Nil);
        assert_eq!(
            RespValue::Map(map),
            resp! { {"x" => "1", 1i64 => 1f64, nil => nil} }
        );
    }

    #[test]
    fn nil() {
        assert_eq!(RespValue::Nil, resp! { nil });
    }

    #[test]
    fn push() {
        assert_eq!(RespValue::Push(vec![]), resp! { [>] });
        assert_eq!(RespValue::Push(vec![RespValue::Nil]), resp! { [> nil] });
        assert_eq!(RespValue::Push(vec![1i64.into()]), resp! { [> 1i64] });
    }

    #[test]
    fn set() {
        // Bytes is a false positive here.
        // <https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type>
        #[allow(clippy::mutable_key_type)]
        let mut set = BTreeSet::new();
        set.insert("x".into());
        set.insert("y".into());
        set.insert(RespPrimitive::Nil);
        assert_eq!(RespValue::Set(set), resp! { {"x", "y", nil} });
    }

    #[test]
    fn verbatim() {
        assert_eq!(
            RespValue::Verbatim("txt".into(), "abc".into()),
            resp! { (= "txt", "abc") }
        );
    }

    #[test]
    fn error() {
        assert_eq!(
            RespValue::Error("ERR stuff".into()),
            resp! { (! "ERR stuff") }
        );
    }

    #[test]
    fn text_values() {
        let value = RespValue::Verbatim("txt".into(), "abc".into());
        assert_eq!(value.text(), Some("abc"));

        let value = RespValue::String("abc".into());
        assert_eq!(value.text(), Some("abc"));

        let value = RespValue::Nil;
        assert_eq!(value.text(), None);

        let value = RespValue::Integer(23);
        assert_eq!(value.text(), None);
    }

    #[test]
    fn error_values() {
        let value = RespValue::Verbatim("txt".into(), "abc".into());
        assert_eq!(value.error(), None);

        let value = RespValue::String("abc".into());
        assert_eq!(value.error(), None);

        let value = RespValue::Nil;
        assert_eq!(value.error(), None);

        let value = RespValue::Integer(23);
        assert_eq!(value.error(), None);

        let value = RespValue::Error("error".into());
        assert_eq!(value.error(), Some("error"));
    }

    #[test]
    fn integer_values() {
        let value = RespValue::Verbatim("txt".into(), "abc".into());
        assert_eq!(value.integer(), None);

        let value = RespValue::String("abc".into());
        assert_eq!(value.integer(), None);

        let value = RespValue::Nil;
        assert_eq!(value.integer(), None);

        let value = RespValue::Integer(23);
        assert_eq!(value.integer(), Some(23));

        let value = RespValue::Error("error".into());
        assert_eq!(value.integer(), None);
    }

    #[test]
    fn array_values() {
        let mut value = RespValue::Verbatim("txt".into(), "abc".into());
        assert_eq!(value.array(), None);

        let mut value = RespValue::String("abc".into());
        assert_eq!(value.array(), None);

        let mut value = RespValue::Nil;
        assert_eq!(value.array(), None);

        let mut value = RespValue::Integer(23);
        assert_eq!(value.array(), None);

        let mut value = RespValue::Error("error".into());
        assert_eq!(value.array(), None);

        let mut value = RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(2)]);
        assert_eq!(
            value.array(),
            Some(&mut vec![RespValue::Integer(1), RespValue::Integer(2)])
        );
    }
}
