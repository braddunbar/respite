use crate::{RespConfig, RespError, RespFrame, RespRequest, RespValue, Splitter};
use async_recursion::async_recursion;
use bytes::{Buf, Bytes, BytesMut};
use std::{
    cmp,
    collections::{BTreeMap, BTreeSet},
    marker::Unpin,
};
use tokio::io::{AsyncRead, AsyncReadExt};

/// A wrapper for [`AsyncRead`] to allow reading a RESP stream, mainly in three ways.
///
/// * Read each frame
/// * Read values, possibly made up of multiple frames
/// * Read requests like a Redis server
#[derive(Debug)]
pub struct RespReader<Inner: AsyncRead + Unpin> {
    /// The input buffer.
    buffer: BytesMut,

    /// Reader config.
    config: RespConfig,

    /// The inner `AsyncRead`.
    inner: Inner,
}

impl<Inner: AsyncRead + Unpin> RespReader<Inner> {
    /// Create a new [`RespReader`] from a byte stream and a [`RespConfig`].
    pub fn new(inner: Inner, config: RespConfig) -> Self {
        Self {
            buffer: BytesMut::default(),
            config,
            inner,
        }
    }

    /// Call `f` for each [`RespRequest`] received on this stream.
    ///
    /// ```
    /// # use tokio::runtime::Runtime;
    /// # use respite::{RespConfig, RespReader, RespRequest};
    /// # let runtime = Runtime::new().unwrap();
    /// # runtime.block_on(async {
    /// let input = "*2\r\n$1\r\na\r\n$1\r\nb\r\n".as_bytes();
    /// let mut reader = RespReader::new(input, RespConfig::default());
    /// let mut requests = Vec::new();
    ///
    /// reader.requests(|request| { requests.push(request); }).await;
    ///
    /// assert!(matches!(requests[0], RespRequest::Argument(_)));
    /// assert!(matches!(requests[1], RespRequest::Argument(_)));
    /// assert!(matches!(requests[2], RespRequest::End));
    /// # });
    /// ```
    pub async fn requests<F>(&mut self, mut f: F)
    where
        F: FnMut(RespRequest),
    {
        if let Err(error) = self.requests_inner(&mut f).await {
            f(RespRequest::Error(error));
        }
    }

    async fn requests_inner<F>(&mut self, f: &mut F) -> Result<(), RespError>
    where
        F: FnMut(RespRequest),
    {
        let mut splitter = Splitter::default();

        while let Some(byte) = self.peek().await? {
            if byte == b'*' {
                self.require("*").await?;
                let size = self.read_size().await?;
                for _ in 0..size {
                    self.require("$").await?;
                    let size = self.read_size().await?;

                    if size > self.config.blob_limit() {
                        return Err(RespError::InvalidBlobLength);
                    }

                    let result = self.read_exact(size).await?;
                    self.require("\r\n").await?;
                    f(result.into());
                }
                f(RespRequest::End);
                continue;
            }

            let line = self.read_line().await?;
            if splitter.split(&line[..]) {
                while let Some(argument) = splitter.next() {
                    f(argument.into());
                }
                f(RespRequest::End);
            } else {
                f(RespRequest::InvalidArgument);
            }
        }

        Ok(())
    }

    /// Read the next [`RespValue`] from the stream.
    ///
    /// ```
    /// # use tokio::runtime::Runtime;
    /// # use respite::{RespConfig, RespValue, RespReader};
    /// # let runtime = Runtime::new().unwrap();
    /// # runtime.block_on(async {
    /// let input = "$3\r\nhi!\r\n".as_bytes();
    /// let mut reader = RespReader::new(input, RespConfig::default());
    /// let frame = reader.value().await.unwrap();
    /// assert_eq!(frame, Some(RespValue::String("hi!".into())));
    /// # });
    /// ```
    #[async_recursion(?Send)]
    pub async fn value(&mut self) -> Result<Option<RespValue>, RespError> {
        let Some(frame) = self.frame().await? else {
            return Ok(None);
        };

        use RespFrame::*;
        let result = match frame {
            Array(size) => {
                let mut array = Vec::new();
                for _ in 0..size {
                    array.push(self.require_frame().await?);
                }
                RespValue::Array(array)
            }
            Attribute(size) => {
                // Bytes is a false positive here.
                // <https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type>
                #[allow(clippy::mutable_key_type)]
                let mut map = BTreeMap::new();
                for _ in 0..size {
                    let key = self.require_frame().await?.try_into()?;
                    let value = self.require_frame().await?;
                    if map.insert(key, value).is_some() {
                        return Err(RespError::InvalidMap);
                    }
                }
                RespValue::Attribute(map)
            }
            Bignum(value) => RespValue::Bignum(value),
            BlobError(value) => RespValue::Error(value),
            Boolean(value) => value.into(),
            BlobString(value) | SimpleString(value) => RespValue::String(value),
            Double(value) => RespValue::Double(value),
            SimpleError(value) => RespValue::Error(value),
            Integer(i) => i.into(),
            Map(size) => {
                // Bytes is a false positive here.
                // <https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type>
                #[allow(clippy::mutable_key_type)]
                let mut map = BTreeMap::new();
                for _ in 0..size {
                    let key = self.require_frame().await?.try_into()?;
                    let value = self.require_frame().await?;
                    if map.insert(key, value).is_some() {
                        return Err(RespError::InvalidMap);
                    }
                }
                RespValue::Map(map)
            }
            Nil => RespValue::Nil,
            Push(size) => {
                let mut push = Vec::new();
                for _ in 0..size {
                    push.push(self.require_frame().await?);
                }
                RespValue::Push(push)
            }
            Set(size) => {
                // Bytes is a false positive here.
                // <https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type>
                #[allow(clippy::mutable_key_type)]
                let mut set = BTreeSet::new();
                for _ in 0..size {
                    let value = self.require_frame().await?.try_into()?;
                    if !set.insert(value) {
                        return Err(RespError::InvalidSet);
                    }
                }
                RespValue::Set(set)
            }
            Verbatim(format, value) => RespValue::Verbatim(format, value),
        };

        Ok(Some(result))
    }

    /// Require one [`RespFrame`] from the stream.
    async fn require_frame(&mut self) -> Result<RespValue, RespError> {
        self.value().await?.ok_or(RespError::EndOfInput)
    }

    /// Read the next [`RespFrame`] from the stream.
    ///
    /// ```
    /// # use tokio::runtime::Runtime;
    /// # use respite::{RespConfig, RespFrame, RespReader};
    /// # let runtime = Runtime::new().unwrap();
    /// # runtime.block_on(async {
    /// let input = "$3\r\nhi!\r\n".as_bytes();
    /// let mut reader = RespReader::new(input, RespConfig::default());
    /// let frame = reader.frame().await.unwrap();
    /// assert_eq!(frame, Some(RespFrame::BlobString("hi!".into())));
    /// # });
    /// ```
    pub async fn frame(&mut self) -> Result<Option<RespFrame>, RespError> {
        let Some(byte) = self.peek().await? else {
            return Ok(None);
        };

        Ok(Some(match byte {
            b'*' => self.read_array().await?,
            b'(' => self.read_bignum().await?,
            b'#' => self.read_boolean().await?,
            b'$' => self.read_blob_string().await?,
            b',' => self.read_double().await?,
            b'-' => self.read_error().await?,
            b':' => self.read_integer().await?,
            b'%' => self.read_map().await?,
            b'_' => self.read_nil().await?,
            b'>' => self.read_push().await?,
            b'~' => self.read_set().await?,
            b'+' => self.read_simple_string().await?,
            b'=' => self.read_verbatim().await?,
            b'!' => self.read_blob_error().await?,
            b'|' => self.read_attribute().await?,
            c => return Err(RespError::UnknownType(c)),
        }))
    }

    /// Read an array.
    async fn read_array(&mut self) -> Result<RespFrame, RespError> {
        self.require("*").await?;
        let size = self.read_size().await?;
        Ok(RespFrame::Array(size))
    }

    /// Read a bignum.
    async fn read_bignum(&mut self) -> Result<RespFrame, RespError> {
        self.require("(").await?;
        let value = self.read_line().await?;
        Ok(RespFrame::Bignum(value))
    }

    /// Read a boolean.
    async fn read_boolean(&mut self) -> Result<RespFrame, RespError> {
        self.require("#").await?;
        let value = match self.pop().await? {
            b't' => true,
            b'f' => false,
            _ => return Err(RespError::InvalidBoolean),
        };
        self.require("\r\n").await?;
        Ok(RespFrame::Boolean(value))
    }

    /// Read a blob string.
    async fn read_blob_string(&mut self) -> Result<RespFrame, RespError> {
        self.require("$").await?;
        if self.peek().await? == Some(b'-') {
            self.require("-1\r\n").await?;
            return Ok(RespFrame::Nil);
        }
        let size = self.read_size().await?;
        if size > self.config.blob_limit() {
            return Err(RespError::InvalidBlobLength);
        }
        let value = self.read_exact(size).await?;
        self.require("\r\n").await?;
        Ok(RespFrame::BlobString(value))
    }

    /// Read a double.
    async fn read_double(&mut self) -> Result<RespFrame, RespError> {
        self.require(",").await?;
        let value = self.read_line().await?;
        let value = std::str::from_utf8(&value[..])
            .ok()
            .and_then(|x| x.parse().ok())
            .ok_or(RespError::InvalidDouble)?;
        Ok(RespFrame::Double(value))
    }

    /// Read an error.
    async fn read_error(&mut self) -> Result<RespFrame, RespError> {
        self.require("-").await?;
        let value = self.read_line().await?;
        Ok(RespFrame::SimpleError(value))
    }

    /// Read an integer.
    async fn read_integer(&mut self) -> Result<RespFrame, RespError> {
        self.require(":").await?;
        let line = self.read_line().await?;
        let value = std::str::from_utf8(&line[..])
            .ok()
            .and_then(|x| x.parse().ok())
            .ok_or(RespError::InvalidInteger)?;
        Ok(RespFrame::Integer(value))
    }

    /// Read a map.
    async fn read_map(&mut self) -> Result<RespFrame, RespError> {
        self.require("%").await?;
        let size = self.read_size().await?;
        Ok(RespFrame::Map(size))
    }

    /// Read a nil.
    async fn read_nil(&mut self) -> Result<RespFrame, RespError> {
        self.require("_\r\n").await?;
        Ok(RespFrame::Nil)
    }

    /// Read a push.
    async fn read_push(&mut self) -> Result<RespFrame, RespError> {
        self.require(">").await?;
        let size = self.read_size().await?;
        Ok(RespFrame::Push(size))
    }

    /// Read a set.
    async fn read_set(&mut self) -> Result<RespFrame, RespError> {
        self.require("~").await?;
        let size = self.read_size().await?;
        Ok(RespFrame::Set(size))
    }

    /// Read a simple string.
    async fn read_simple_string(&mut self) -> Result<RespFrame, RespError> {
        self.require("+").await?;
        let value = self.read_line().await?;
        Ok(RespFrame::SimpleString(value))
    }

    /// Read a verbatim.
    async fn read_verbatim(&mut self) -> Result<RespFrame, RespError> {
        self.require("=").await?;
        let size = self.read_size().await?;
        if size > self.config.blob_limit() {
            return Err(RespError::InvalidBlobLength);
        }
        if size < 4 {
            return Err(RespError::InvalidVerbatim);
        }
        let value = self.read_exact(size).await?;
        if value.get(3) != Some(&b':') {
            return Err(RespError::InvalidVerbatim);
        }
        let format = value.slice(..3);
        let value = value.slice(4..);
        self.require("\r\n").await?;
        Ok(RespFrame::Verbatim(format, value))
    }

    /// Read a blob error.
    async fn read_blob_error(&mut self) -> Result<RespFrame, RespError> {
        self.require("!").await?;
        let size = self.read_size().await?;
        if size > self.config.blob_limit() {
            return Err(RespError::InvalidBlobLength);
        }
        let value = self.read_exact(size).await?;
        self.require("\r\n").await?;
        Ok(RespFrame::BlobError(value))
    }

    /// Read an attribute.
    async fn read_attribute(&mut self) -> Result<RespFrame, RespError> {
        self.require("|").await?;
        let size = self.read_size().await?;
        Ok(RespFrame::Attribute(size))
    }

    /// Try to read some data from `inner`.
    async fn read(&mut self) -> Result<usize, RespError> {
        Ok(self.inner.read_buf(&mut self.buffer).await?)
    }

    /// Read one byte.
    async fn pop(&mut self) -> Result<u8, RespError> {
        if self.buffer.is_empty() {
            self.read_some().await?;
        }
        Ok(self.buffer.get_u8())
    }

    /// Try to read some data from `inner`. Return an error if we've reached the end of the input.
    async fn read_some(&mut self) -> Result<(), RespError> {
        if self.read().await? == 0 {
            return Err(RespError::EndOfInput);
        }

        Ok(())
    }

    /// Read a size.
    async fn read_size(&mut self) -> Result<usize, RespError> {
        let mut size = 0;

        if self.peek().await? == Some(b'\r') {
            return Err(RespError::InvalidBlobLength);
        }

        loop {
            match self.pop().await? {
                b'\r' => {
                    self.require("\n").await?;
                    return Ok(size);
                }
                b @ b'0'..=b'9' => {
                    let n = (b - b'0').into();
                    size = size
                        .checked_mul(10)
                        .and_then(|size| size.checked_add(n))
                        .ok_or(RespError::InvalidBlobLength)?;
                }
                _ => return Err(RespError::InvalidBlobLength),
            }
        }
    }

    /// Require a specific sequence of bytes and consume them.
    async fn require<E>(&mut self, expected: E) -> Result<(), RespError>
    where
        E: AsRef<[u8]> + Send + Sync,
    {
        for expected in expected.as_ref() {
            let got = self.pop().await?;

            if got != *expected {
                return Err(RespError::Unexpected(*expected, got));
            }
        }

        Ok(())
    }

    /// Read an entire line.
    async fn read_line(&mut self) -> Result<Bytes, RespError> {
        let mut from = 0;
        let slice = loop {
            let to = cmp::min(self.config.inline_limit(), self.buffer.len());
            let index = self.buffer[from..to].iter().position(|&b| b == b'\r');

            if let Some(index) = index {
                break self.buffer.split_to(from + index);
            }

            if self.buffer.len() > self.config.inline_limit() {
                return Err(RespError::TooBigInline);
            }

            from = self.buffer.len();
            self.read_some().await?;
        };

        self.require("\r\n").await?;
        Ok(slice.freeze())
    }

    /// Read an exact number of bytes.
    async fn read_exact(&mut self, len: usize) -> Result<Bytes, RespError> {
        self.buffer.reserve(len);
        while self.buffer.len() < len {
            self.read_some().await?;
        }
        Ok(self.buffer.split_to(len).freeze())
    }

    /// Peek at the next byte in the stream.
    async fn peek(&mut self) -> Result<Option<u8>, RespError> {
        if self.buffer.is_empty() && self.read().await? == 0 {
            return Ok(None);
        }

        Ok(Some(self.buffer[0]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::VecDeque;

    macro_rules! assert_frame {
        ($input:expr, $expected:expr) => {{
            let mut reader = RespReader::new($input.as_bytes(), RespConfig::default());
            let value = reader.frame().await;
            let value = value.expect("must be Ok(…)");
            let value = value.expect("mut be Some(_)");
            assert_eq!(value, $expected);
        }};
    }

    macro_rules! assert_frame_error {
        ($input:expr, $expected:pat) => {{
            assert_frame_error!($input, $expected, RespConfig::default())
        }};
        ($input:expr, $expected:pat, $config:expr) => {{
            let mut reader = RespReader::new($input.as_bytes(), $config);
            let value = reader.frame().await;
            let value = value.expect_err("must be Err(…)");
            assert!(matches!(value, $expected));
        }};
    }

    macro_rules! assert_value {
        ($input:expr, $expected:tt) => {{
            let mut reader = RespReader::new($input.as_bytes(), RespConfig::default());
            let value = reader.value().await;
            let value = value.expect("must be Ok(…)");
            assert_eq!(value, Some(resp! { $expected }));
        }};
    }

    macro_rules! assert_value_error {
        ($input:expr, $expected:pat) => {{
            let mut reader = RespReader::new($input.as_bytes(), RespConfig::default());
            let value = reader.value().await;
            let value = value.expect_err("must be Err(…)");
            assert!(matches!(value, $expected));
        }};
    }

    #[tokio::test]
    async fn returns_none() -> Result<(), RespError> {
        let mut reader = RespReader::new("+OK\r\n".as_bytes(), RespConfig::default());
        assert_eq!(
            reader.frame().await.unwrap(),
            Some(RespFrame::SimpleString("OK".into()))
        );
        assert_eq!(reader.frame().await.unwrap(), None);
        Ok(())
    }

    #[tokio::test]
    async fn array_frame() -> Result<(), RespError> {
        assert_frame!("*0\r\n", RespFrame::Array(0));
        assert_frame!("*1\r\n", RespFrame::Array(1));
        assert_frame_error!("*\r\n", RespError::InvalidBlobLength);
        assert_frame_error!("*1", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn bignum_frame() -> Result<(), RespError> {
        assert_frame!("(123\r\n", RespFrame::Bignum("123".into()));
        assert_frame_error!("(123", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn boolean_frame() -> Result<(), RespError> {
        assert_frame!("#t\r\n", RespFrame::Boolean(true));
        assert_frame!("#f\r\n", RespFrame::Boolean(false));
        assert_frame_error!("#x\r\n", RespError::InvalidBoolean);
        assert_frame_error!("#t", RespError::EndOfInput);
        assert_frame_error!("#tx", RespError::Unexpected(b'\r', b'x'));
        Ok(())
    }

    #[tokio::test]
    async fn blob_string_frame() -> Result<(), RespError> {
        assert_frame!("$5\r\nabcde\r\n", RespFrame::BlobString("abcde".into()));
        assert_frame!("$-1\r\n", RespFrame::Nil);
        assert_frame_error!("$-1", RespError::EndOfInput);
        assert_frame_error!("$2", RespError::EndOfInput);
        assert_frame_error!("$\r\n\r\n", RespError::InvalidBlobLength);
        let mut config = RespConfig::default();
        config.set_blob_limit(5);
        assert_frame_error!(
            "$10\r\n1234567890\r\n",
            RespError::InvalidBlobLength,
            config
        );
        Ok(())
    }

    #[tokio::test]
    async fn double_frame() -> Result<(), RespError> {
        assert_frame!(",5.4\r\n", RespFrame::Double(5.4f64.into()));
        assert_frame!(",inf\r\n", RespFrame::Double(f64::INFINITY.into()));
        assert_frame!(",-inf\r\n", RespFrame::Double(f64::NEG_INFINITY.into()));
        assert_frame_error!(",invalid\r\n", RespError::InvalidDouble);
        assert_frame_error!(",5.4", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn error_frame() -> Result<(), RespError> {
        assert_frame!("-ERR x\r\n", RespFrame::SimpleError("ERR x".into()));
        assert_frame_error!("-ERR x", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn integer_frame() -> Result<(), RespError> {
        assert_frame!(":4\r\n", RespFrame::Integer(4i64));
        assert_frame!(":-4\r\n", RespFrame::Integer(-4i64));
        assert_frame_error!(":invalid\r\n", RespError::InvalidInteger);
        assert_frame_error!(":4", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn map_frame() -> Result<(), RespError> {
        assert_frame!("%4\r\n", RespFrame::Map(4));
        assert_frame_error!("%invalid\r\n", RespError::InvalidBlobLength);
        assert_frame_error!("%4", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn attribute_frame() -> Result<(), RespError> {
        assert_frame!("|4\r\n", RespFrame::Attribute(4));
        assert_frame_error!("|invalid\r\n", RespError::InvalidBlobLength);
        assert_frame_error!("|4", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn nil_frame() -> Result<(), RespError> {
        assert_frame!("_\r\n", RespFrame::Nil);
        assert_frame_error!("_", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn push_frame() -> Result<(), RespError> {
        assert_frame!(">3\r\n", RespFrame::Push(3));
        assert_frame!(">32\r\n", RespFrame::Push(32));
        assert_frame_error!(">invalid\r\n", RespError::InvalidBlobLength);
        assert_frame_error!(">3", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn set_frame() -> Result<(), RespError> {
        assert_frame!("~2\r\n", RespFrame::Set(2));
        assert_frame!("~32\r\n", RespFrame::Set(32));
        assert_frame_error!("~invalid\r\n", RespError::InvalidBlobLength);
        assert_frame_error!("~3", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn simple_string_frame() -> Result<(), RespError> {
        assert_frame!("+abc\r\n", RespFrame::SimpleString("abc".into()));
        assert_frame!("+\r\n", RespFrame::SimpleString("".into()));
        assert_frame_error!("+", RespError::EndOfInput);
        Ok(())
    }

    #[tokio::test]
    async fn verbatim_frame() -> Result<(), RespError> {
        assert_frame!(
            "=7\r\ntxt:abc\r\n",
            RespFrame::Verbatim("txt".into(), "abc".into())
        );
        assert_frame_error!("=2\r\ntx\r\n", RespError::InvalidVerbatim);
        assert_frame_error!("=5\r\ntxt x\r\n", RespError::InvalidVerbatim);
        assert_frame_error!("=invalid\r\ntxt x\r\n", RespError::InvalidBlobLength);
        assert_frame_error!("=5\r\ntxt:x", RespError::EndOfInput);
        assert_frame_error!("=5", RespError::EndOfInput);
        let mut config = RespConfig::default();
        config.set_blob_limit(5);
        assert_frame_error!(
            "=10\r\ntxt:123456\r\n",
            RespError::InvalidBlobLength,
            config
        );
        Ok(())
    }

    #[tokio::test]
    async fn blob_error_frame() -> Result<(), RespError> {
        assert_frame!("!4\r\ntest\r\n", RespFrame::BlobError("test".into()));
        assert_frame_error!("!invalid\r\ntx\r\n", RespError::InvalidBlobLength);
        assert_frame_error!("!4\r\n", RespError::EndOfInput);
        assert_frame_error!("!4", RespError::EndOfInput);
        let mut config = RespConfig::default();
        config.set_blob_limit(5);
        assert_frame_error!(
            "!10\r\n1234567890\r\n",
            RespError::InvalidBlobLength,
            config
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_size() -> Result<(), RespError> {
        let mut reader = RespReader::new("1234\r\n".as_bytes(), RespConfig::default());
        assert!(matches!(reader.read_size().await, Ok(1234)));

        Ok(())
    }

    #[tokio::test]
    async fn read_size_invalid() -> Result<(), RespError> {
        let mut reader = RespReader::new("invalid\r\n".as_bytes(), RespConfig::default());
        assert!(matches!(
            reader.read_size().await,
            Err(RespError::InvalidBlobLength)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn read_some_end_of_input() -> Result<(), RespError> {
        let mut reader = RespReader::new("".as_bytes(), RespConfig::default());
        assert!(matches!(
            reader.read_some().await,
            Err(RespError::EndOfInput)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn pop() -> Result<(), RespError> {
        let mut reader = RespReader::new("abcde".as_bytes(), RespConfig::default());
        assert!(matches!(reader.pop().await, Ok(b'a')));
        assert!(matches!(reader.pop().await, Ok(b'b')));
        assert!(matches!(reader.pop().await, Ok(b'c')));
        assert!(matches!(reader.pop().await, Ok(b'd')));
        assert!(matches!(reader.pop().await, Ok(b'e')));
        assert!(matches!(reader.pop().await, Err(RespError::EndOfInput)));

        Ok(())
    }

    #[tokio::test]
    async fn require() -> Result<(), RespError> {
        let mut reader = RespReader::new("abcf".as_bytes(), RespConfig::default());
        assert!(matches!(reader.require("ab").await, Ok(())));
        assert!(matches!(
            reader.require("cd").await,
            Err(RespError::Unexpected(b'd', b'f'))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn read_line() -> Result<(), RespError> {
        let mut reader = RespReader::new("abcdefg\r\n".as_bytes(), RespConfig::default());
        assert_eq!(
            reader.read_line().await.unwrap(),
            Bytes::from_static(b"abcdefg")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_line_malformed_crlf() -> Result<(), RespError> {
        let mut reader = RespReader::new("abcdefg\rxxxxx".as_bytes(), RespConfig::default());
        assert!(matches!(
            reader.read_line().await,
            Err(RespError::Unexpected(b'\n', b'x'))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn read_exact() -> Result<(), RespError> {
        let mut reader = RespReader::new("abcdefgxxxxxxxxxxxxxx".as_bytes(), RespConfig::default());
        assert_eq!(
            reader.read_exact(7).await.unwrap(),
            Bytes::from_static(b"abcdefg")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_exact_end_of_input() -> Result<(), RespError> {
        let mut reader = RespReader::new("abcd".as_bytes(), RespConfig::default());
        assert!(matches!(
            reader.read_exact(7).await,
            Err(RespError::EndOfInput)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn peek() -> Result<(), RespError> {
        let mut reader = RespReader::new("a".as_bytes(), RespConfig::default());
        assert_eq!(reader.peek().await.unwrap(), Some(b'a'));
        assert_eq!(reader.pop().await.unwrap(), b'a');
        assert_eq!(reader.peek().await.unwrap(), None);

        Ok(())
    }

    #[tokio::test]
    async fn read_array_value() -> Result<(), RespError> {
        assert_value!("*2\r\n$3\r\nfoo\r\n#t\r\n", ["foo", true]);
        assert_value!("*3\r\n$1\r\nx\r\n$-1\r\n$-1\r\n", ["x", nil, nil]);
        Ok(())
    }

    #[tokio::test]
    async fn read_bignum_value() -> Result<(), RespError> {
        assert_value!("(123\r\n", (big "123"));
        Ok(())
    }

    #[tokio::test]
    async fn read_simple_string_value() -> Result<(), RespError> {
        assert_value!("+foo\r\n", "foo");
        assert_value!("*2\r\n+foo\r\n#t\r\n", ["foo", true]);
        Ok(())
    }

    #[tokio::test]
    async fn read_map_value() -> Result<(), RespError> {
        assert_value!("%2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n:2\r\n", {"foo" => 1, "bar" => 2});
        Ok(())
    }

    #[tokio::test]
    async fn read_set_value() -> Result<(), RespError> {
        assert_value!("~2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", {"foo", "bar"});
        Ok(())
    }

    #[tokio::test]
    async fn invalid_map() -> Result<(), RespError> {
        assert_value_error!(
            "%2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nfoo\r\n:2\r\n",
            RespError::InvalidMap
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_set() -> Result<(), RespError> {
        assert_value_error!("~2\r\n$3\r\nfoo\r\n$3\r\nfoo\r\n", RespError::InvalidSet);
        Ok(())
    }

    #[tokio::test]
    async fn read_nil_value() -> Result<(), RespError> {
        assert_value!("*2\r\n_\r\n_\r\n", [nil, nil]);
        Ok(())
    }

    #[tokio::test]
    async fn read_push_value() -> Result<(), RespError> {
        assert_value!(">2\r\n+one\r\n+two\r\n", [> "one", "two"]);
        Ok(())
    }

    #[tokio::test]
    async fn read_double_value() -> Result<(), RespError> {
        assert_value!(",2.5\r\n", 2.5f64);
        Ok(())
    }

    #[tokio::test]
    async fn read_verbatim_value() -> Result<(), RespError> {
        assert_value!("=7\r\ntxt:abc\r\n", (= "txt", "abc"));
        assert_value!("*2\r\n=7\r\ntxt:abc\r\n:1\r\n", [(= "txt", "abc"), 1i64]);
        Ok(())
    }

    #[tokio::test]
    async fn read_string_value() -> Result<(), RespError> {
        assert_value!("$-1\r\n", nil);
        assert_value!("$3\r\nabc\r\n", "abc");
        Ok(())
    }

    #[tokio::test]
    async fn read_error() -> Result<(), RespError> {
        assert_value!("-ERR foo\r\n", (!"ERR foo"));
        Ok(())
    }

    #[tokio::test]
    async fn read_attribute_value() -> Result<(), RespError> {
        assert_value!("|2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n:2\r\n", {a "foo" => 1, "bar" => 2});
        Ok(())
    }

    macro_rules! request_messages {
        ($input:expr) => {{
            request_messages!($input, RespConfig::default())
        }};
        ($input:expr, $config:expr) => {{
            let mut reader = RespReader::new(&$input[..], $config);
            let mut messages = VecDeque::new();
            reader.requests(|message| messages.push_back(message)).await;
            messages
        }};
    }

    macro_rules! assert_none {
        ($messages:expr) => {
            let value = $messages.pop_front();
            if !value.is_none() {
                panic!("expected none, got: {:?}", value);
            }
        };
    }

    macro_rules! assert_argument {
        ($messages:expr, $expected:expr) => {
            let value = $messages.pop_front().unwrap();
            match value {
                RespRequest::Argument(argument) => {
                    assert_eq!(&argument[..], &$expected[..]);
                }
                _ => panic!(
                    "expected {:?}, got {:?}",
                    RespRequest::Argument(Bytes::from_static($expected)),
                    value
                ),
            }
        };
    }

    macro_rules! assert_ready {
        ($messages:expr) => {
            let value = $messages.pop_front().unwrap();
            match value {
                RespRequest::End => {}
                _ => panic!("expected {:?}, got: {:?}", RespRequest::End, value),
            }
        };
    }

    macro_rules! assert_invalid_argument {
        ($messages:expr) => {
            let value = $messages.pop_front().unwrap();
            match value {
                RespRequest::InvalidArgument => {}
                _ => panic!(
                    "expected {:?}, got: {:?}",
                    RespRequest::InvalidArgument,
                    value
                ),
            }
        };
    }

    macro_rules! assert_error {
        ($messages:expr, $expected:pat) => {
            let value = $messages.pop_front().unwrap();
            assert!(matches!(value, RespRequest::Error($expected)));
        };
    }

    #[tokio::test]
    async fn read_array_request() -> Result<(), RespError> {
        let mut messages = request_messages!(b"*2\r\n$1\r\nx\r\n$2\r\nab\r\n*1\r\n$1\r\nz\r\n");
        assert_argument!(messages, b"x");
        assert_argument!(messages, b"ab");
        assert_ready!(messages);
        assert_argument!(messages, b"z");
        assert_ready!(messages);
        assert_none!(messages);
        assert_none!(messages);

        Ok(())
    }

    #[tokio::test]
    async fn read_inline_request() -> Result<(), RespError> {
        let mut messages = request_messages!(b"foo bar\r\nbaz bam\r\n");
        assert_argument!(messages, b"foo");
        assert_argument!(messages, b"bar");
        assert_ready!(messages);
        assert_argument!(messages, b"baz");
        assert_argument!(messages, b"bam");
        assert_ready!(messages);
        assert_none!(messages);
        assert_none!(messages);

        Ok(())
    }

    #[tokio::test]
    async fn read_invalid_argument() -> Result<(), RespError> {
        let mut messages = request_messages!(b"foo 'bar\r\nbaz bam\r\nfoo\r\n");
        assert_invalid_argument!(messages);
        assert_argument!(messages, b"baz");
        assert_argument!(messages, b"bam");
        assert_ready!(messages);
        assert_argument!(messages, b"foo");
        assert_ready!(messages);
        assert_none!(messages);
        assert_none!(messages);

        Ok(())
    }

    #[tokio::test]
    async fn read_invalid_blob_string() -> Result<(), RespError> {
        let mut messages = request_messages!(b"*2\r\n$1\r\nx\r\n$invalid\r\nasdf\r\n");
        assert_argument!(messages, b"x");
        assert_error!(messages, RespError::InvalidBlobLength);

        Ok(())
    }

    #[tokio::test]
    async fn read_invalid_end_of_input() -> Result<(), RespError> {
        let mut messages = request_messages!(b"*2\r\n$1\r\nx\r\n$1\r\ny");
        assert_argument!(messages, b"x");
        assert_error!(messages, RespError::EndOfInput);

        Ok(())
    }

    #[tokio::test]
    async fn read_too_long_blob_string() -> Result<(), RespError> {
        let mut config = RespConfig::default();
        config.set_blob_limit(5);
        let mut messages = request_messages!(b"*2\r\n$1\r\nx\r\n$10\r\n1234567890\r\n", config);
        assert_argument!(messages, b"x");
        assert_error!(messages, RespError::InvalidBlobLength);

        Ok(())
    }

    #[tokio::test]
    async fn read_too_long_inline() -> Result<(), RespError> {
        let mut config = RespConfig::default();
        config.set_inline_limit(5);
        let mut messages = request_messages!(b"1234567890\r\n", config);
        assert_error!(messages, RespError::TooBigInline);

        Ok(())
    }
}
