#!/bin/bash
cargo fmt
cargo run -- -c test/config.yml export > test/database.json
jq '.' test/database.json