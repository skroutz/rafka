#!/bin/bash
set -e

/usr/lib/go-1.8/bin/go build

ulimit -c unlimited

exec "./rafka" "$@"
