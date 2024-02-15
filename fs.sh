#!/bin/bash
release=0

export RUST_LOG="warn,fs2cloud=info,fs2cloud::store=debug,fs2cloud::controller::mount=trace"
mkdir -p /tmp/fuse
sudo umount /tmp/fuse

if [ $release -eq 0 ]; then
  echo "debug mode"
  cargo run -- -c test/config.yml mount -m /tmp/fuse
else
  echo "release mode"
  cargo run --release -- -c test/config.yml mount -m /tmp/fuse
fi