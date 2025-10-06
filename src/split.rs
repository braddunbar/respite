use bytes::{Buf, Bytes, BytesMut};
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
pub fn split(mut line: BytesMut, arguments: &mut VecDeque<Bytes>) -> bool {
    use State::*;

    let mut state = Trim;
    let mut from = 0;
    let mut to = 0;

    macro_rules! invalid {
        () => {{
            arguments.clear();
            return false;
        }};
    }

    macro_rules! arg {
        () => {{
            arguments.push_back(line.split_to(to).freeze());
        }};
    }

    macro_rules! push {
        ($c:expr) => {{
            line[to] = $c;
            to += 1;
            from += 1;
        }};
    }

    loop {
        match state {
            Trim => match &line[from..] {
                [] => {
                    return true;
                }
                [b'\'', ..] => {
                    state = SingleQuotes;
                    line.advance(1);
                }
                [b'"', ..] => {
                    state = DoubleQuotes;
                    line.advance(1);
                }
                [b, ..] if b.is_ascii_whitespace() => {
                    line.advance(1);
                }
                _ => {
                    state = NoQuotes;
                }
            },
            NoQuotes => match &line[from..] {
                [] => {
                    arg!();
                    return true;
                }
                [b, ..] if b.is_ascii_whitespace() => {
                    state = Trim;
                    arg!();
                    from = 0;
                    to = 0;
                }
                _ => {
                    from += 1;
                    to += 1;
                }
            },
            SingleQuotes => match &line[from..] {
                [] => {
                    invalid!();
                }
                [b'\'', b, ..] if !b.is_ascii_whitespace() => {
                    invalid!();
                }
                [b'\'', ..] => {
                    state = Trim;
                    arg!();
                    line.advance(1);
                    from -= to;
                    to = 0;
                }
                [b'\\', b'\'', ..] => {
                    from += 1;
                    push!(b'\'');
                }
                [b, ..] => {
                    push!(*b);
                }
            },
            DoubleQuotes => match &line[from..] {
                [] => {
                    invalid!();
                }
                [b'"', b, ..] if !b.is_ascii_whitespace() => {
                    invalid!();
                }
                [b'"', ..] => {
                    state = Trim;
                    arg!();
                    line.advance(1);
                    from -= to;
                    to = 0;
                }
                [b'\\', b'x', a, b, ..] => {
                    let a = *a;
                    let b = *b;
                    let string = std::str::from_utf8(&line[from + 2..from + 4]).ok();
                    let byte = string.and_then(|string| u8::from_str_radix(string, 16).ok());

                    if let Some(byte) = byte {
                        from += 3;
                        push!(byte);
                    } else {
                        from += 1;
                        push!(b'x');
                        push!(a);
                        push!(b);
                    }
                }
                [b'\\', b, ..] => {
                    from += 1;
                    push!(match b {
                        b'a' => b'\x07',
                        b'b' => b'\x08',
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        _ => *b,
                    });
                }
                [b, ..] => {
                    push!(*b);
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_split {
        ($input:expr) => {
            let mut arguments = VecDeque::default();
            let input = BytesMut::from(&$input[..]);
            assert!(split(input, &mut arguments));
            assert!(arguments.is_empty());
        };
        ($input:expr, $($expected:expr),*) => {
            let mut arguments = VecDeque::default();
            let input = BytesMut::from(&$input[..]);
            assert!(split(input, &mut arguments));
            let actual: Vec<Bytes> = arguments.iter().cloned().collect();
            let expected = vec![ $( Bytes::from(&$expected[..]) ),* ];
            assert_eq!(actual, expected);
        };
    }

    macro_rules! assert_no_split {
        ($input:expr) => {
            let mut arguments = VecDeque::default();
            let input = BytesMut::from(&$input[..]);
            assert!(!split(input, &mut arguments));
            assert!(arguments.is_empty());
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
