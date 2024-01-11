use bytes::Bytes;
use ordered_float::OrderedFloat;

/// A single frame in a RESP stream.
#[derive(Debug, Eq, PartialEq)]
pub enum RespFrame {
    Array(usize),
    Attribute(usize),
    Bignum(Bytes),
    BlobError(Bytes),
    BlobString(Bytes),
    Boolean(bool),
    Double(OrderedFloat<f64>),
    Integer(i64),
    Map(usize),
    Nil,
    Push(usize),
    Set(usize),
    SimpleError(Bytes),
    SimpleString(Bytes),
    Verbatim(Bytes, Bytes),
}
