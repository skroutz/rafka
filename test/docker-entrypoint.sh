#!/bin/bash
set -e

go build -race

ulimit -c unlimited

exec "./rafka" "$@"
