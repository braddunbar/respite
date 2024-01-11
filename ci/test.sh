#!/usr/bin/env sh

set -ex

cargo build --release
cargo test --quiet
