#!/usr/bin/env sh

set -ex

cargo fmt --all -- --check
cargo clippy --all --tests -- \
  -D clippy::all \
  -D clippy::dbg_macro \
  -D warnings
