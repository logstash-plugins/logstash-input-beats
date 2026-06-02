#!/bin/bash

set -euo pipefail

run_with_optional_sudo() {
    if command -v sudo >/dev/null 2>&1; then
        sudo "$@"
    else
        "$@"
    fi
}

if ! command -v apt >/dev/null 2>&1; then
    echo "Expected apt on GitHub Actions runner, but it was not found" >&2
    exit 1
fi

run_with_optional_sudo apt install -y openssl
