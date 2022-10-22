#!/bin/bash
export RUST_LOG="warn,fs2cloud::fuse=trace"
mkdir -p /tmp/fuse
sudo umount /tmp/fuse
cargo fmt
cargo run -- -c test/config.yml fuse -m /tmp/fuse