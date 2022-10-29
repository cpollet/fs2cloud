#!/bin/bash
export RUST_LOG="warn,fs2cloud::controller::mount=trace"
mkdir -p /tmp/fuse
sudo umount /tmp/fuse
cargo run -- -c test/config.yml mount -m /tmp/fuse