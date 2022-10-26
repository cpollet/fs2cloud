#!/bin/bash
export RUST_LOG="warn,fs2cloud::fuse=trace"
mkdir -p /tmp/fuse
sudo umount /tmp/fuse
cargo run -- -c test/config.yml mount -m /tmp/fuse