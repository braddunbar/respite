use crate::{RespConfig, RespError, RespFrame, RespRequest, RespValue, Splitter};
use bytes::{Buf, Bytes, BytesMut};
use futures::stream::{BoxStream, StreamExt, unfold};
use std::{
    cmp,
    collections::{BTreeMap, BTreeSet},
    marker::Unpin,
};
use tokio::io::{AsyncRead, AsyncReadExt};

/// The state of a reader when reading a stream of [`RespRequest`].
#[derive(Debug)]
enum RequestState {
    /// Parsing arguments.
    Args(usize),

    /// No more requests are in the stream.
    Done,

    /// The initial state, waiting for input.
    Init,

    /// Splitting an inline request.
    Splitting,
}

/// A wrapper for [`AsyncRead`] to allow reading a RESP stream, mainly in three ways.
///
/// * Read each frame
/// * Read values, possibly made up of multiple frames
/// * Read requests like a Redis server
#[derive(Debug)]
pub struct RespReader<Inner: AsyncRead + Unpin + Send + 'static> {
    /// The input buffer.
    buffer: BytesMut,

    /// Reader config.
    config: RespConfig,

    /// The inner `AsyncRead`.
    inner: Inner,

    /// A `Splitter` for splitting arguments.
    splitter: Splitter,

    /// The current request state.
    request_state: RequestState,
}

impl<Inner: AsyncRead + Unpin + Send + 'static> RespReader<Inner> {
    /// Create a new [`RespReader`] from a byte stream and a [`RespConfig`].
    pub fn new(inner: Inner, config: RespConfig) -> Self {
        Self {
            buffer: BytesMut::default(),
            config,
            inner,
            splitter: Splitter::default(),
            request_state: RequestState::Init,
        }
    }

    /// A cancel-safe [`Stream`] of [`RespRequest`].
    pub fn requests(self) -> BoxStream<'static, RespRequest> {
        unfold(Some(self), |reader| async move {
            let mut reader = reader?;
            match reader.request().await.transpose()? {
                Ok(request) => Some((request, Some(reader))),
                Err(error) => Some((RespRequest::Error(error), None)),
            }
        })
        .fuse()
        .boxed()
    }

    /// Read the next [`RespRequest`] from the stream.
    pub async fn request(&mut self) -> Result<Option<RespRequest>, RespError> {
        loop {
            use RequestState::*;
            match self.request_state {
                Args(0) => {
                    self.request_state = Init;
                    return Ok(Some(RespRequest::End));
                }
                Args(c) => {
                    self.require("$").await?;
                    let size = self.read_size().await?;

                    if size > self.config.blob_limit() {
                        return Err(RespError::InvalidBlobLength);
                    }

                    let result = self.read_exact(size).await?;
                    self.require("\r\n").await?;
                    self.request_state = Args(c - 1);
                    return Ok(Some(result.into()));
                }
                Init => {
                    let Some(byte) = self.peek().await? else {
                        self.request_state = Done;
                        return Ok(None);
                    };

                    if byte == b'*' {
                        self.require("*").await?;
                        self.request_state = Args(self.read_size().await?);
                        continue;
                    }

                    let line = self.read_line().await?;
                    if self.splitter.split(&line[..]) {
                        self.request_state = Splitting;
                        continue;
                    }

                    return Ok(Some(RespRequest::InvalidArgument));
                }
                Splitting => {
                    if let Some(argument) = self.splitter.next() {
                        return Ok(Some(argument.into()));
                    }
                    self.request_state = Init;
                    return Ok(Some(RespRequest::End));
                }
                Done => {
                    return Ok(None);
                }
            }
        }
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
    pub async fn value(&mut self) -> Result<Option<RespValue>, RespError> {
        let Some(frame) = self.frame().await? else {
            return Ok(None);
        };

        use RespFrame::*;
        let result = match frame {
            Array(size) => {
                let mut array = Vec::new();
                for _ in 0..size {
                    array.push(Box::pin(self.require_value()).await?);
                }
                RespValue::Array(array)
            }
            Attribute(size) => {
                // Bytes is a false positive here.
                // <https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type>
                #[allow(clippy::mutable_key_type)]
                let mut map = BTreeMap::new();
                for _ in 0..size {
                    let key = Box::pin(self.require_value()).await?.try_into()?;
                    let value = Box::pin(self.require_value()).await?;
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
                    let key = Box::pin(self.require_value()).await?.try_into()?;
                    let value = Box::pin(self.require_value()).await?;
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
                    push.push(Box::pin(self.require_value()).await?);
                }
                RespValue::Push(push)
            }
            Set(size) => {
                // Bytes is a false positive here.
                // <https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type>
                #[allow(clippy::mutable_key_type)]
                let mut set = BTreeSet::new();
                for _ in 0..size {
                    let value = Box::pin(self.require_value()).await?.try_into()?;
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

    /// A cancel-safe [`Stream`] of `RespValue`.
    pub fn values(self) -> BoxStream<'static, Result<RespValue, RespError>> {
        unfold(self, |mut reader| async move {
            let item = reader.value().await.transpose()?;
            Some((item, reader))
        })
        .fuse()
        .boxed()
    }

    /// Require one [`RespFrame`] from the stream.
    async fn require_value(&mut self) -> Result<RespValue, RespError> {
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

    /// A cancel-safe [`Stream`] of `RespFrame`.
    pub fn frames(self) -> BoxStream<'static, Result<RespFrame, RespError>> {
        unfold(self, |mut reader| async move {
            let item = reader.frame().await.transpose()?;
            Some((item, reader))
        })
        .fuse()
        .boxed()
    }

    /// Read an array.
    async fn read_array(&mut self) -> Result<RespFrame, RespError> {
        self.require("*").await?;
        if self.peek().await? == Some(b'-') {
            self.require("-1\r\n").await?;
            return Ok(RespFrame::Nil);
        }
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

    async fn require_line(&mut self) -> Result<BytesMut, RespError> {
        let mut from = 0;
        loop {
            let to = cmp::min(self.config.inline_limit(), self.buffer.len());
            let index = self.buffer[from..to].iter().position(|&b| b == b'\r');

            if let Some(index) = index {
                let line = self.buffer.split_to(from + index);
                self.require("\r\n").await?;
                return Ok(line);
            }

            if self.buffer.len() > self.config.inline_limit() {
                return Err(RespError::TooBigInline);
            }

            from = self.buffer.len();
            self.read_some().await?;
        }
    }

    /// Read an entire line.
    async fn read_line(&mut self) -> Result<Bytes, RespError> {
        Ok(self.require_line().await?.freeze())
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
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::{io::AsyncWriteExt, time::timeout};

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
        ($input:expr, $expected:pat) => {{ assert_frame_error!($input, $expected, RespConfig::default()) }};
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
        assert_frame!("*-1\r\n", RespFrame::Nil);
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
        assert_frame!(",5.4e1\r\n", RespFrame::Double(54f64.into()));
        assert_frame!(",5.4e+1\r\n", RespFrame::Double(54f64.into()));
        assert_frame!(",5.4e-1\r\n", RespFrame::Double(0.54f64.into()));
        assert_frame!(",5.4E1\r\n", RespFrame::Double(54f64.into()));
        assert_frame!(",5.4E+1\r\n", RespFrame::Double(54f64.into()));
        assert_frame!(",5.4E-1\r\n", RespFrame::Double(0.54f64.into()));
        assert_frame!(",inf\r\n", RespFrame::Double(f64::INFINITY.into()));
        assert_frame!(",-inf\r\n", RespFrame::Double(f64::NEG_INFINITY.into()));
        assert_frame!(",nan\r\n", RespFrame::Double(f64::NAN.into()));
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

    macro_rules! assert_requests {
        ($config:expr, $input:expr, $($expected:tt),*) => {
            let reader = RespReader::new(&$input[..], $config);
            let mut stream = reader.requests();
            $(resp_request!(stream, $expected);)*
            assert!(stream.next().await.is_none());
            assert!(stream.next().await.is_none());
        };
    }

    macro_rules! resp_request {
        ($stream:expr, (error $error:pat)) => {
            let value = $stream.next().await;
            let Some(value) = value else {
                panic!("expected end but got none");
            };
            let RespRequest::Error(error) = value else {
                panic!("expected error but got {:?}", value);
            };
            assert!(matches!(error, $error));
        };
        ($stream:expr, invalid) => {
            let value = $stream.next().await;
            let Some(value) = value else {
                panic!("expected end but got none");
            };
            assert!(matches!(value, RespRequest::InvalidArgument));
        };
        ($stream:expr, end) => {
            let value = $stream.next().await;
            let Some(value) = value else {
                panic!("expected end but got none");
            };
            assert!(matches!(value, RespRequest::End));
        };
        ($stream:expr, $argument:expr) => {
            let value = $stream.next().await;
            let Some(value) = value else {
                panic!("expected {:?} but got none", $argument.escape_ascii());
            };
            let RespRequest::Argument(value) = value else {
                panic!(
                    "expected {:?} but got {:?}",
                    $argument.escape_ascii(),
                    value
                );
            };
            assert_eq!(value, Bytes::from(&$argument[..]));
        };
    }

    #[tokio::test]
    async fn read_array_requests() -> Result<(), RespError> {
        assert_requests!(
            RespConfig::default(),
            b"*2\r\n$1\r\nx\r\n$2\r\nab\r\n*1\r\n$1\r\nz\r\n",
            b"x",
            b"ab",
            end,
            b"z",
            end
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_inline_request() -> Result<(), RespError> {
        assert_requests!(
            RespConfig::default(),
            b"foo bar\r\nbaz bam\r\n",
            b"foo",
            b"bar",
            end,
            b"baz",
            b"bam",
            end
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_invalid_argument() -> Result<(), RespError> {
        assert_requests!(
            RespConfig::default(),
            b"foo 'bar\r\nbaz bam\r\nfoo\r\n",
            invalid,
            b"baz",
            b"bam",
            end,
            b"foo",
            end
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_invalid_blob_string() -> Result<(), RespError> {
        assert_requests!(
            RespConfig::default(),
            b"*2\r\n$1\r\nx\r\n$invalid\r\nasdf\r\n",
            b"x",
            (error RespError::InvalidBlobLength)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_invalid_end_of_input() -> Result<(), RespError> {
        assert_requests!(
            RespConfig::default(),
            b"*2\r\n$1\r\nx\r\n$1\r\ny",
            b"x",
            (error RespError::EndOfInput)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_too_long_blob_string() -> Result<(), RespError> {
        let mut config = RespConfig::default();
        config.set_blob_limit(5);
        assert_requests!(
            config,
            b"*2\r\n$1\r\nx\r\n$10\r\n1234567890\r\n",
            b"x",
            (error RespError::InvalidBlobLength)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_too_long_inline() -> Result<(), RespError> {
        let mut config = RespConfig::default();
        config.set_inline_limit(5);
        assert_requests!(
            config,
            b"1234567890\r\n",
            (error RespError::TooBigInline)
        );

        Ok(())
    }

    #[tokio::test]
    async fn values_cancel_safety() -> Result<(), RespError> {
        let (reader, mut writer) = tokio::io::simplex(100000);
        let reader = RespReader::new(reader, RespConfig::default());
        let values = reader.values();
        tokio::pin!(values);
        let duration = Duration::from_millis(5);
        writer.write_all(b"*2\r\n:1\r\n").await?;
        assert!(timeout(duration, values.next()).await.is_err());
        writer.write_all(b":2\r\n").await?;
        assert_eq!(
            values.next().await.unwrap().unwrap(),
            RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(2)])
        );
        Ok(())
    }

    #[tokio::test]
    async fn frames_cancel_safety() -> Result<(), RespError> {
        let (reader, mut writer) = tokio::io::simplex(100000);
        let reader = RespReader::new(reader, RespConfig::default());
        let frames = reader.frames();
        tokio::pin!(frames);
        let duration = Duration::from_millis(5);
        writer.write_all(b":1234").await?;
        assert!(timeout(duration, frames.next()).await.is_err());
        writer.write_all(b"5678\r\n").await?;
        assert_eq!(
            frames.next().await.unwrap().unwrap(),
            RespFrame::Integer(12345678)
        );
        Ok(())
    }
}
