/// A version of the RESP protocol.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum RespVersion {
    V2,
    V3,
}

impl std::fmt::Display for RespVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespVersion::V2 => write!(f, "2"),
            RespVersion::V3 => write!(f, "3"),
        }
    }
}

impl From<RespVersion> for u8 {
    fn from(version: RespVersion) -> u8 {
        match version {
            RespVersion::V2 => 2,
            RespVersion::V3 => 3,
        }
    }
}
