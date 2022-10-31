#!/bin/bash
export RUST_LOG="warn,fs2cloud=info,fs2cloud::store::s3_official=trace"
rm test/database.db3
cargo run --release -- -c test/config.yml push -f test/data