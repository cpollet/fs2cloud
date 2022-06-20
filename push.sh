#!/bin/bash
rm test/database.db3
cargo fmt
time cargo run -- push -f test/data -c test/config.yml