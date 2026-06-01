#!/bin/bash

set -euo pipefail

if command -v sudo >/dev/null 2>&1; then
    CMD_PREFIX=(sudo)
else
    CMD_PREFIX=()
fi

if command -v apt >/dev/null 2>&1; then
    "${CMD_PREFIX[@]}" apt install -y openssl
elif command -v microdnf >/dev/null 2>&1; then
    "${CMD_PREFIX[@]}" microdnf install -y openssl
else
    echo "No supported package manager found (expected apt or microdnf)" >&2
    exit 1
fi
