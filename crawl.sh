#!/bin/bash
release=0

export RUST_LOG="warn,fs2cloud=info,fs2cloud::store::s3_official=trace"
rm test/database.db3

if [ $release -eq 0 ]; then
  echo "debug mode"
  cargo build
  time target/debug/fs2cloud -c test/config.yml crawl
else
  echo "release mode"
  cargo build --release
  time target/release/fs2cloud -c test/config.yml crawl
fi