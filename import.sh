#!/bin/bash
rm test/database.db3
cat test/database.json | cargo run -- -c test/config.yml import