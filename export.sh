#!/bin/bash
cargo fmt
rm test/database.json
cargo run -- -c test/config.yml export > test/database.json
jq '.' test/database.json