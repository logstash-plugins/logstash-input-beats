#!/bin/bash

set -euo pipefail

run_with_optional_sudo() {
    if command -v sudo >/dev/null 2>&1; then
        sudo "$@"
    else
        "$@"
    fi
}

if command -v apt >/dev/null 2>&1; then
    run_with_optional_sudo apt install -y openssl
elif command -v microdnf >/dev/null 2>&1; then
    run_with_optional_sudo microdnf install -y openssl
else
    echo "No supported package manager found (expected apt or microdnf)" >&2
    exit 1
fi
