//! Abstractions for reading the RESP protocol.
//!
//! You can read a RESP stream in several ways. Which one is appropriate depends on your goals.
//!
//! # Frames
//!
//! With [`RespReader::frame`], you can read each individual frame from a RESP stream and decide
//! what to do with it. This allows you to process streams without buffering.
//!
//! # Requests
//!
//! [`RespReader::requests`] will pass each component of a Redis-style request to a closure you
//! provide. This allows for easily sending each argument over a channel to another task.
//!
//! # Values
//!
//! You can also use [`RespReader::value`], which will buffer values and return a whole tree of
//! frames for arrays, maps, sets, etc. This is primarily meant for testing purposes, but could
//! also be useful in cases where performance isn't super important.

/// Conveniently create a [`RespPrimitive`]
#[macro_export]
macro_rules! resp_primitive {
    (nil) => {{
        use $crate::RespPrimitive;
        RespPrimitive::Nil
    }};
    ($x:tt) => {{ $x.into() }};
}

/// Conveniently create a [`RespValue`]
#[macro_export]
macro_rules! resp {
    ( ( ! $error:expr ) ) => {{
        use $crate::RespValue;
        RespValue::Error($error.into())
    }};
    ( ( = $format:expr, $text:expr ) ) => {{
        use $crate::RespValue;
        RespValue::Verbatim($format.into(), $text.into())
    }};
    ( [ > $($x:tt),* ] ) => {{
        use $crate::RespValue;
        RespValue::Push(vec![$( resp!{ $x } ),*])
    }};
    ( [ $($x:tt),* ] ) => {{
        use $crate::RespValue;
        RespValue::Array(vec![$( resp!{ $x } ),*])
    }};
    ( ( big $x:tt ) ) => {{
        use $crate::RespValue;
        RespValue::Bignum($x.into())
    }};
    ( { } ) => {{
        use $crate::RespValue;
        use std::collections::BTreeMap;
        RespValue::Map(BTreeMap::new())
    }};
    ( { ~ } ) => {{
        use $crate::RespValue;
        use std::collections::BTreeSet;
        RespValue::Set(BTreeSet::new())
    }};
    ( { $($key:tt => $value:tt),* } ) => {{
        use $crate::RespValue;
        use std::collections::BTreeMap;

        // Bytes is a false positive here.
        // <https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type>
        #[allow(clippy::mutable_key_type)]
        let mut map = BTreeMap::new();
        $(map.insert(resp_primitive!{ $key }, resp!{ $value });)*
        RespValue::Map(map)
    }};
    ( { $($x:tt),* } ) => {{
        use $crate::RespValue;
        use std::collections::BTreeSet;

        // Bytes is a false positive here.
        // <https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type>
        #[allow(clippy::mutable_key_type)]
        let mut set = BTreeSet::new();
        $(set.insert(resp_primitive!{ $x });)*
        RespValue::Set(set)
    }};
    ( {a $($key:tt => $value:tt),* } ) => {{
        use $crate::RespValue;
        use std::collections::BTreeMap;

        // Bytes is a false positive here.
        // <https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type>
        #[allow(clippy::mutable_key_type)]
        let mut map = BTreeMap::new();
        $(map.insert(resp_primitive!{ $key }, resp!{ $value });)*
        RespValue::Attribute(map)
    }};
    (nil) => {{
        use $crate::RespValue;
        RespValue::Nil
    }};
    ($x:tt) => {{
        $x.into()
    }};
}

mod config;
mod error;
mod frame;
mod primitive;
mod reader;
mod request;
mod split;
mod value;
mod version;
mod writer;

pub use config::RespConfig;
pub use error::RespError;
pub use frame::RespFrame;
pub use primitive::RespPrimitive;
pub use reader::RespReader;
pub use request::RespRequest;
use split::split;
pub use value::RespValue;
pub use version::RespVersion;
pub use writer::RespWriter;
