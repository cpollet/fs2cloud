#!/bin/bash
rm test/database.db3
cargo fmt
cargo run -- -c test/config.yml push -f test/data