# Respite

An async reader for RESP streams.

[![Crates.io][crates-badge]][crates-url]
[![Build Status][ci-badge]][ci-url]

[crates-badge]: https://img.shields.io/crates/v/respite.svg
[crates-url]: https://crates.io/crates/respite
[ci-badge]: https://github.com/braddunbar/respite/workflows/CI/badge.svg
[ci-url]: https://github.com/braddunbar/respite/actions

## Usage

To use `respite`, add this to your `Cargo.toml`:

```toml
[dependencies]
respite = "*"
```

Next, add this to your crate:

```rust
use respite::RespReader;
```
