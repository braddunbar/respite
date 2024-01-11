use crate::RespError;
use bytes::Bytes;

/// One piece of a RESP request, split into pieces for sending through a channel.
#[derive(Debug)]
pub enum RespRequest {
    /// One argument in a RESP request.
    Argument(Bytes),

    /// An invalid argument in an inline request.
    InvalidArgument,

    /// A RESP protocol error.
    Error(RespError),

    /// Notification of the end of a request.
    End,
}

impl From<Bytes> for RespRequest {
    fn from(value: Bytes) -> Self {
        RespRequest::Argument(value)
    }
}
