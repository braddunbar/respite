# Respite

Some tools for reading and writing [RESP][RESP] streams.

[RESP]: https://redis.io/docs/reference/protocol-spec/

[![Crates.io][crates-badge]][crates-url]
[![Docs][docs-badge]][docs-url]
[![Build Status][ci-badge]][ci-url]

[crates-badge]: https://img.shields.io/crates/v/respite.svg
[crates-url]: https://crates.io/crates/respite
[ci-badge]: https://github.com/braddunbar/respite/workflows/CI/badge.svg
[ci-url]: https://github.com/braddunbar/respite/actions
[docs-badge]: https://img.shields.io/docsrs/respite/latest.svg
[docs-url]: https://docs.rs/respite/

## Usage

To use `respite`, add this to your `Cargo.toml`:

```toml
[dependencies]
respite = "*"
```

Next, add this to your crate:

```rust
use respite::{RespReader, RespWriter, RespVersion};
```
