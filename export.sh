#!/bin/bash
cargo fmt
time cargo run -- -c test/config.yml export