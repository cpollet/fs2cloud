#!/bin/bash
export RUST_LOG="warn,fs2cloud=info,fs2cloud::store::s3_official=trace"
rm test/database.db3
cargo build # --release
time target/release/fs2cloud -c test/config.yml push