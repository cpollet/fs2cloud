#!/bin/bash
rm test/database.db3
cargo fmt
cat test/database.json | cargo run -- -c test/config.yml import