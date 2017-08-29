#!/bin/bash
set -e

/usr/lib/go-1.8/bin/go build -race

ulimit -c unlimited

exec "./rafka" "$@"
