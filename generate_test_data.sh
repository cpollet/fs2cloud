#!/usr/bin/env bash

M="000000"

mkdir -p test/data/folder
echo -n "." >  test/data/1B
echo "Generate random 1M"
head -c "1$M" /dev/urandom > test/data/folder/1M
echo "Generate random 2M"
head -c "2$M" /dev/urandom > test/data/folder/2M
echo "Generate random 10M"
head -c "10$M" /dev/urandom > test/data/10M
echo "Generate random 100M"
head -c "100$M" /dev/urandom > test/data/100M
echo "Generate random 1000M"
head -c "1000$M" /dev/urandom > test/data/1000M