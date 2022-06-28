#!/usr/bin/env bash

mkdir -p test/data/folder
echo "Generate random 1M"
head -c 1M /dev/urandom > test/data/folder/1M
echo "Generate random 10M"
head -c 10M /dev/urandom > test/data/10M
echo "Generate random 100M"
head -c 100M /dev/urandom > test/data/100M