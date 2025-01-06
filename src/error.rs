use thiserror::Error;

/// An error encountered while reading a RESP stream.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RespError {
    /// Reached the end of the stream unexpectedly
    #[error("unexpected end of input")]
    EndOfInput,

    /// Received an invalid boolean
    #[error("invalid boolean")]
    InvalidBoolean,

    /// Received an invalid blob
    #[error("invalid blob length")]
    InvalidBlobLength,

    /// Received an invalid double
    #[error("invalid double")]
    InvalidDouble,

    /// Received an invalid integer
    #[error("invalid integer")]
    InvalidInteger,

    /// Received an invalid map
    #[error("invalid map")]
    InvalidMap,

    /// Received an invalid set
    #[error("invalid set")]
    InvalidSet,

    /// Received an invalid verbatim
    #[error("invalid verbatim")]
    InvalidVerbatim,

    /// Error reading from the stream.
    #[error("io error")]
    IO(#[from] std::io::Error),

    /// Simple frame cannot contain a newline.
    #[error("newline is not allowed in this frame")]
    Newline,

    /// Unsupported in current version.
    #[error("unsupported in the current version")]
    Version,

    /// Expected a primitive, but got a complex value
    #[error("map keys and set values must be primitives")]
    RespPrimitive,

    /// Received an inline request that was too big.
    #[error("too big inline request")]
    TooBigInline,

    /// Unexpected byte sequence
    #[error("expected {:?}, got {:?}", char::from(*.0), char::from(*.1))]
    Unexpected(u8, u8),

    /// Unknown RESP type
    #[error("unknown resp type: {:?}", char::from(*.0))]
    UnknownType(u8),

    /// Invalid inline command
    #[error("invalid inline command")]
    InvalidInline,
}
