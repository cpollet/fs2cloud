#!/bin/bash
release=0

export RUST_BACKTRACE=1
export RUST_LOG="warn,fs2cloud=info,fs2cloud::store=debug,fs2cloud::controller::pull=debug"

rm -r test/pull/10M

if [ $release -eq 0 ]; then
  echo "debug mode"
  cargo build && time target/debug/fs2cloud -c test/config.yml pull --from 10M --to test/pull
else
  echo "release mode"
  cargo build --release && time target/release/fs2cloud -c test/config.yml pull --from 10M --to test/pull
fi