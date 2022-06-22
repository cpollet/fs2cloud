#!/bin/bash
export RUST_LOG="warn,fs2cloud::fuse=debug"
mkdir -p /tmp/fuse
sudo umount /tmp/fuse
cargo fmt
time cargo run -- -c test/config.yml fuse -m /tmp/fuse