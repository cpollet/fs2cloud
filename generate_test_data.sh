#!/usr/bin/env bash

mkdir -p test/data/folder
head -c 1M /dev/urandom > test/data/folder/1M
head -c 10M /dev/urandom > test/data/10M
