#!/bin/bash
rm test/database.db3
cargo fmt
time cargo run -- -c test/config.yml push -f test/data