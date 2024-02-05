use crate::{RespError, RespVersion};
use std::io::Write;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// A wrapper for [`AsyncWrite`] to allow writing a RESP stream.
#[derive(Debug)]
pub struct RespWriter<Inner: AsyncWrite + Unpin> {
    /// A buffer for writing output
    buffer: Vec<u8>,

    /// The inner `AsyncWrite`.
    inner: Inner,

    /// The current version.
    pub version: RespVersion,
}

macro_rules! write_all {
    ($self:expr, $value:expr) => {{
        $self.inner.write_all($value).await?;
    }};
}

macro_rules! write_fmt {
    ($self:expr, $($tail:tt)*) => {{
        $self.buffer.clear();
        write!($self.buffer, $( $tail )*).unwrap();
        write_all!($self, &$self.buffer[..]);
    }};
}

impl<Inner: AsyncWrite + Unpin> RespWriter<Inner> {
    /// Create a new [`RespWriter`] from an [`AsyncWrite`].
    pub fn new(inner: Inner) -> Self {
        Self {
            buffer: Vec::new(),
            inner,
            version: RespVersion::V2,
        }
    }

    /// Flush the inner writer.
    pub async fn flush(&mut self) -> Result<(), RespError> {
        self.inner.flush().await?;
        Ok(())
    }

    /// Write an array frame.
    pub async fn write_array(&mut self, len: usize) -> Result<(), RespError> {
        write_fmt!(self, "*{}\r\n", len);
        Ok(())
    }

    /// Write an attribute frame.
    pub async fn write_attribute(&mut self, value: &[u8]) -> Result<(), RespError> {
        if self.v2() {
            return Err(RespError::Version);
        }
        write_fmt!(self, "|{}\r\n", value.len());
        write_all!(self, value);
        write_all!(self, b"\r\n");
        Ok(())
    }

    /// Write a bignum frame.
    pub async fn write_bignum(&mut self, value: &[u8]) -> Result<(), RespError> {
        if value.contains(&b'\n') {
            return Err(RespError::Newline);
        }
        match self.v3() {
            true => write_all!(self, b"("),
            false => write_all!(self, b"+"),
        }
        write_all!(self, value);
        write_all!(self, b"\r\n");
        Ok(())
    }

    /// Write a blob error frame.
    pub async fn write_blob_error(&mut self, value: &[u8]) -> Result<(), RespError> {
        if self.v2() {
            return Err(RespError::Version);
        }
        write_fmt!(self, "!{}\r\n", value.len());
        write_all!(self, value);
        write_all!(self, b"\r\n");
        Ok(())
    }

    /// Write a blob string frame.
    pub async fn write_blob_string(&mut self, value: &[u8]) -> Result<(), RespError> {
        write_fmt!(self, "${}\r\n", value.len());
        write_all!(self, value);
        write_all!(self, b"\r\n");
        Ok(())
    }

    /// Write a boolean frame.
    pub async fn write_boolean(&mut self, value: bool) -> Result<(), RespError> {
        let bytes = match (self.v3(), value) {
            (true, true) => b"#t\r\n",
            (true, false) => b"#f\r\n",
            (false, true) => b":1\r\n",
            (false, false) => b":0\r\n",
        };
        write_all!(self, bytes);
        Ok(())
    }

    /// Write a double frame.
    pub async fn write_double(&mut self, value: f64) -> Result<(), RespError> {
        match self.v3() {
            true => write_fmt!(self, ",{}\r\n", value),
            false => write_fmt!(self, "+{}\r\n", value),
        }
        Ok(())
    }

    /// Write an integer frame.
    pub async fn write_integer(&mut self, value: i64) -> Result<(), RespError> {
        write_fmt!(self, ":{}\r\n", value);
        Ok(())
    }

    /// Write a nil frame.
    pub async fn write_nil(&mut self) -> Result<(), RespError> {
        match self.v3() {
            true => write_all!(self, b"_\r\n"),
            false => write_all!(self, b"$-1\r\n"),
        }
        Ok(())
    }

    /// Write a map frame.
    pub async fn write_map(&mut self, len: usize) -> Result<(), RespError> {
        match self.v3() {
            true => write_fmt!(self, "%{}\r\n", len),
            false => write_fmt!(self, "*{}\r\n", 2 * len),
        }
        Ok(())
    }

    /// Write a push frame.
    pub async fn write_push(&mut self, len: usize) -> Result<(), RespError> {
        match self.v3() {
            true => write_fmt!(self, ">{}\r\n", len),
            false => write_fmt!(self, "*{}\r\n", len),
        }
        Ok(())
    }

    /// Write a set frame.
    pub async fn write_set(&mut self, len: usize) -> Result<(), RespError> {
        match self.v3() {
            true => write_fmt!(self, "~{}\r\n", len),
            false => write_fmt!(self, "*{}\r\n", len),
        }
        Ok(())
    }

    /// Write a simple error frame.
    pub async fn write_simple_error(&mut self, value: &[u8]) -> Result<(), RespError> {
        if value.contains(&b'\n') {
            return Err(RespError::Newline);
        }
        write_all!(self, b"-");
        write_all!(self, value);
        write_all!(self, b"\r\n");
        Ok(())
    }

    /// Write a simple string frame.
    pub async fn write_simple_string(&mut self, value: &[u8]) -> Result<(), RespError> {
        if value.contains(&b'\n') {
            return Err(RespError::Newline);
        }
        write_all!(self, b"+");
        write_all!(self, value);
        write_all!(self, b"\r\n");
        Ok(())
    }

    /// Write a verbatim frame.
    pub async fn write_verbatim(&mut self, format: &[u8], value: &[u8]) -> Result<(), RespError> {
        if self.v3() {
            write_fmt!(self, "={}\r\n", format.len() + 1 + value.len());
            write_all!(self, format);
            write_all!(self, b":");
            write_all!(self, value);
            write_all!(self, b"\r\n");
        } else {
            write_fmt!(self, "${}\r\n", value.len());
            write_all!(self, value);
            write_all!(self, b"\r\n");
        }
        Ok(())
    }

    /// Is the current version V2?
    fn v2(&self) -> bool {
        self.version == RespVersion::V2
    }

    /// Is the current version V3?
    fn v3(&self) -> bool {
        self.version == RespVersion::V3
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::from_utf8;

    macro_rules! assert_write {
        ($f:ident ( $($arg:expr),* ), $expected:expr, $version:expr) => {{
            let mut output = Vec::new();
            let mut writer = RespWriter::new(&mut output);
            writer.version = $version;
            writer.$f($($arg),*).await?;
            drop(writer);
            match (from_utf8(&output[..]), from_utf8($expected)) {
                (Ok(a), Ok(b)) => assert_eq!(a, b),
                _ => assert_eq!(&output[..], $expected),
            }
        }};
    }

    macro_rules! assert_write2 {
        ($f:ident ( $($arg:expr),* ), $expected:expr) => {{
            assert_write!($f( $($arg),* ), $expected, RespVersion::V2)
        }};
    }

    macro_rules! assert_write3 {
        ($f:ident ( $($arg:expr),* ), $expected:expr) => {{
            assert_write!($f( $($arg),* ), $expected, RespVersion::V3)
        }};
    }

    macro_rules! assert_error {
        ($f:ident ( $($arg:expr),* ), $expected:pat, $version:expr) => {{
            let mut output = Vec::new();
            let mut writer = RespWriter::new(&mut output);
            writer.version = $version;
            let error = writer.$f($($arg),*).await.expect_err("got Ok(_)");
            drop(writer);
            assert!(matches!(error, $expected));
        }};
    }

    macro_rules! assert_error2 {
        ($f:ident ( $($arg:expr),* ), $expected:pat) => {{
            assert_error!($f($($arg),*), $expected, RespVersion::V2)
        }};
    }

    macro_rules! assert_error3 {
        ($f:ident ( $($arg:expr),* ), $expected:pat) => {{
            assert_error!($f($($arg),*), $expected, RespVersion::V3)
        }};
    }

    #[tokio::test]
    async fn write_nil() -> Result<(), RespError> {
        assert_write2!(write_nil(), b"$-1\r\n");
        assert_write3!(write_nil(), b"_\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_array() -> Result<(), RespError> {
        assert_write2!(write_array(0), b"*0\r\n");
        assert_write2!(write_array(1), b"*1\r\n");
        assert_write2!(write_array(73), b"*73\r\n");
        assert_write3!(write_array(0), b"*0\r\n");
        assert_write3!(write_array(1), b"*1\r\n");
        assert_write3!(write_array(73), b"*73\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_attribute() -> Result<(), RespError> {
        assert_error2!(write_attribute("test".as_bytes()), RespError::Version);
        assert_write3!(write_attribute("test".as_bytes()), b"|4\r\ntest\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_bignum() -> Result<(), RespError> {
        assert_write2!(write_bignum("12345".as_bytes()), b"+12345\r\n");
        assert_error2!(write_bignum("123\n45".as_bytes()), RespError::Newline);
        assert_write3!(write_bignum("12345".as_bytes()), b"(12345\r\n");
        assert_error3!(write_bignum("123\n45".as_bytes()), RespError::Newline);
        Ok(())
    }

    #[tokio::test]
    async fn write_blob_error() -> Result<(), RespError> {
        assert_error2!(write_blob_error("ERR x".as_bytes()), RespError::Version);
        assert_write3!(write_blob_error("ERR x".as_bytes()), b"!5\r\nERR x\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_blob_string() -> Result<(), RespError> {
        assert_write2!(write_blob_string("12345".as_bytes()), b"$5\r\n12345\r\n");
        assert_write3!(write_blob_string("12345".as_bytes()), b"$5\r\n12345\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_boolean() -> Result<(), RespError> {
        assert_write2!(write_boolean(true), b":1\r\n");
        assert_write2!(write_boolean(false), b":0\r\n");
        assert_write3!(write_boolean(true), b"#t\r\n");
        assert_write3!(write_boolean(false), b"#f\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_double() -> Result<(), RespError> {
        assert_write2!(write_double(1.23f64), b"+1.23\r\n");
        assert_write3!(write_double(1.23f64), b",1.23\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_integer() -> Result<(), RespError> {
        assert_write2!(write_integer(1023), b":1023\r\n");
        assert_write2!(write_integer(-15), b":-15\r\n");
        assert_write3!(write_integer(1023), b":1023\r\n");
        assert_write3!(write_integer(-15), b":-15\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_map() -> Result<(), RespError> {
        assert_write2!(write_map(1023), b"*2046\r\n");
        assert_write2!(write_map(15), b"*30\r\n");
        assert_write3!(write_map(1023), b"%1023\r\n");
        assert_write3!(write_map(15), b"%15\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_push() -> Result<(), RespError> {
        assert_write2!(write_push(1023), b"*1023\r\n");
        assert_write2!(write_push(15), b"*15\r\n");
        assert_write3!(write_push(1023), b">1023\r\n");
        assert_write3!(write_push(15), b">15\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_set() -> Result<(), RespError> {
        assert_write2!(write_set(1023), b"*1023\r\n");
        assert_write2!(write_set(15), b"*15\r\n");
        assert_write3!(write_set(1023), b"~1023\r\n");
        assert_write3!(write_set(15), b"~15\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_simple_error() -> Result<(), RespError> {
        assert_write2!(write_simple_error("ERR x".as_bytes()), b"-ERR x\r\n");
        assert_write3!(write_simple_error("ERR x".as_bytes()), b"-ERR x\r\n");
        assert_error2!(write_simple_error("ERR\nx".as_bytes()), RespError::Newline);
        assert_error3!(write_simple_error("ERR\nx".as_bytes()), RespError::Newline);
        Ok(())
    }

    #[tokio::test]
    async fn write_simple_string() -> Result<(), RespError> {
        assert_write2!(write_simple_string("foo".as_bytes()), b"+foo\r\n");
        assert_write3!(write_simple_string("foo".as_bytes()), b"+foo\r\n");
        assert_error2!(
            write_simple_string("new\nline".as_bytes()),
            RespError::Newline
        );
        assert_error3!(
            write_simple_string("new\nline".as_bytes()),
            RespError::Newline
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_verbatim() -> Result<(), RespError> {
        assert_write2!(
            write_verbatim("txt".as_bytes(), "1234567890".as_bytes()),
            b"$10\r\n1234567890\r\n"
        );
        assert_write3!(
            write_verbatim("txt".as_bytes(), "1234567890".as_bytes()),
            b"=14\r\ntxt:1234567890\r\n"
        );
        Ok(())
    }
}
