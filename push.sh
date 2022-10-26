#!/bin/bash
export RUST_LOG="warn,fs2cloud=info"
rm test/database.db3
cargo run --release -- -c test/config.yml push -f test/data