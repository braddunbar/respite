use bytes::{BufMut, Bytes, BytesMut};
use std::collections::VecDeque;

/// Different modes of splitting arguments
#[derive(Debug)]
enum State {
    /// Trimming whitespace outside of arguments
    Trim,

    /// Reading an unquoted argument
    NoQuotes,

    /// Reading a single quoted argument
    SingleQuotes,

    /// Reading a double quoted argument
    DoubleQuotes,
}

/// A single line argument iterator.
#[derive(Debug, Default)]
pub struct Splitter {
    arguments: VecDeque<Bytes>,
    buffer: BytesMut,
}

/// Split an inline request into arguments.
///
/// * Unquoted whitespace is trimmed and discarded.
/// * Unquoted arguments are read verbatim, without escapes.
/// * Single quoted arguments can include spaces and escaped quotes.
/// * Double quoted arguments can include spaces and various escaped characters.
///   * Hex: `\xff\x00`
///   * Tab: `\t`
///   * Newline: `\n`
///   * Carriage Return: `\r`
///   * Backspace: `\b`
///   * Alert/Bell: `\a`
impl Splitter {
    pub fn next(&mut self) -> Option<Bytes> {
        self.arguments.pop_front()
    }

    pub fn split(&mut self, mut input: &[u8]) -> bool {
        use State::*;

        let mut state = Trim;
        self.buffer.reserve(input.len());

        macro_rules! invalid {
            () => {{
                self.arguments.clear();
                self.buffer.clear();
                return false;
            }};
        }

        macro_rules! push {
            () => {{
                self.arguments.push_back(self.buffer.split().freeze());
            }};
        }

        loop {
            input = match state {
                Trim => match input {
                    [] => {
                        return true;
                    }
                    [b'\'', rest @ ..] => {
                        state = SingleQuotes;
                        rest
                    }
                    [b'"', rest @ ..] => {
                        state = DoubleQuotes;
                        rest
                    }
                    [b, rest @ ..] if b.is_ascii_whitespace() => rest,
                    _ => {
                        state = NoQuotes;
                        continue;
                    }
                },
                NoQuotes => match input {
                    [] => {
                        push!();
                        return true;
                    }
                    [b, rest @ ..] if b.is_ascii_whitespace() => {
                        state = Trim;
                        push!();
                        rest
                    }
                    [b, rest @ ..] => {
                        self.buffer.put_u8(*b);
                        rest
                    }
                },
                SingleQuotes => match input {
                    [] => {
                        invalid!();
                    }
                    [b'\'', b, ..] if !b.is_ascii_whitespace() => {
                        invalid!();
                    }
                    [b'\'', rest @ ..] => {
                        state = Trim;
                        push!();
                        rest
                    }
                    [b'\\', b'\'', rest @ ..] => {
                        self.buffer.put_u8(b'\'');
                        rest
                    }
                    [b, rest @ ..] => {
                        self.buffer.put_u8(*b);
                        rest
                    }
                },
                DoubleQuotes => match input {
                    [] => {
                        invalid!();
                    }
                    [b'"', b, ..] if !b.is_ascii_whitespace() => {
                        invalid!();
                    }
                    [b'"', rest @ ..] => {
                        state = Trim;
                        push!();
                        rest
                    }
                    [b'\\', b'x', a, b, rest @ ..] => {
                        let array = &[*a, *b][..];
                        let string = std::str::from_utf8(array).ok();
                        let byte = string.and_then(|string| u8::from_str_radix(string, 16).ok());

                        if let Some(byte) = byte {
                            self.buffer.put_u8(byte);
                        } else {
                            self.buffer.put_u8(b'x');
                            self.buffer.put_u8(*a);
                            self.buffer.put_u8(*b);
                        }

                        rest
                    }
                    [b'\\', b, rest @ ..] => {
                        self.buffer.put_u8(match b {
                            b'a' => b'\x07',
                            b'b' => b'\x08',
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            _ => *b,
                        });
                        rest
                    }
                    [b, rest @ ..] => {
                        self.buffer.put_u8(*b);
                        rest
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_split {
        ($input:expr) => {
            let mut splitter = Splitter::default();
            assert!(splitter.split(&$input[..]));
            assert_eq!(splitter.next(), None);
        };
        ($input:expr, $($expected:expr),*) => {
            let mut splitter = Splitter::default();
            assert!(splitter.split(&$input[..]));
            let mut actual = Vec::new();
            while let Some(argument) = splitter.next() {
                actual.push(argument);
            }
            let expected = vec![ $( Bytes::from(&$expected[..]) ),* ];
            assert_eq!(actual, expected);
        };
    }

    macro_rules! assert_no_split {
        ($input:expr) => {
            let mut splitter = Splitter::default();
            assert!(!splitter.split(&$input[..]));
            assert_eq!(None, splitter.next());
            assert!(splitter.buffer.is_empty());
        };
    }

    #[test]
    fn empty() {
        assert_split!(b"");
        assert_split!(b"  ");
    }

    #[test]
    fn trim() {
        assert_split!(b"     get   y ", b"get", b"y");
        assert_split!(b"   y ", b"y");
        assert_split!(b"   \"y\" ", b"y");
        assert_split!(b"\t\r   \"y\"\r\n", b"y");
        assert_split!(b" \"x\"  'y'   z ", b"x", b"y", b"z");
    }

    #[test]
    fn one_argument() {
        assert_split!(b"get", b"get");
    }

    #[test]
    fn multiple_arguments() {
        assert_split!(b"get x y", b"get", b"x", b"y");
        assert_split!(b" x y", b"x", b"y");
    }

    #[test]
    fn single_quotes() {
        assert_split!(b" 'x' ", b"x");
        assert_split!(b" '\\'' ", b"'");
        assert_split!(b" '\\x' ", b"\\x");

        assert_no_split!(b" 'x'y ");
    }

    #[test]
    fn double_quotes() {
        assert_split!(b" \"\\\"\\r\\n\\t\" ", b"\"\r\n\t");
        assert_split!(b"\"x\"", b"x");
        assert_split!(b"\"\\x11\"", b"\x11");
        assert_split!(b"\"\\xzz\"", b"xzz");

        assert_no_split!(b" \"x\"y");
    }

    #[test]
    fn alarm() {
        assert_split!(b" \"\\a\" ", b"\x07");
    }

    #[test]
    fn backspace() {
        assert_split!(b" \"\\b\" ", b"\x08");
    }
}
